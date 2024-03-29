// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"

	"cloud.google.com/go/cloudsqlconn"
	"cloud.google.com/go/logging"
	"example.com/micro/metadata"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	_ "github.com/go-sql-driver/mysql"
)

type App struct {
	*http.Server
	projectID string
	log       *logging.Logger
	db        *sql.DB
}

func main() {
	ctx := context.Background()
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("listening on port %s", port)
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	app, err := newApp(ctx, port, projectID)
	if err != nil {
		log.Fatalf("unable to initialize application: %v", err)
	}

	log.Println("starting HTTP server")
	go func() {
		if err := app.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server closed: %v", err)
		}
	}()

	// Listen for SIGINT to gracefully shutdown.
	nctx, stop := signal.NotifyContext(ctx, os.Interrupt, os.Kill)
	defer stop()
	<-nctx.Done()
	log.Println("shutdown initiated")

	// Cloud Run gives apps 10 seconds to shut down. See
	// https://cloud.google.com/blog/topics/developers-practitioners/graceful-shutdowns-cloud-run-deep-dive
	// for more details.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = app.Shutdown(ctx)
	if err != nil {
		return
	}
	log.Println("shutdown")
}

func newApp(ctx context.Context, port, projectID string) (*App, error) {
	app := &App{
		Server: &http.Server{
			Addr: ":" + port,
			// Add some defaults, should be changed to suit your use case.
			ReadTimeout:    10 * time.Second,
			WriteTimeout:   10 * time.Second,
			MaxHeaderBytes: 1 << 20,
		},
	}
	app.getDatabase()

	if projectID == "" {
		projID, err := metadata.ProjectID()
		if err != nil {
			return nil, fmt.Errorf("unable to detect Project ID from GOOGLE_CLOUD_PROJECT or metadata server: %w", err)
		}
		projectID = projID
	}
	app.projectID = projectID

	client, err := logging.NewClient(ctx, fmt.Sprintf("projects/%s", app.projectID),
		// We don't need to make any requests when logging to stderr.
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		))
	if err != nil {
		return nil, fmt.Errorf("unable to initialize logging client: %v", err)
	}
	app.log = client.Logger("test-log", logging.RedirectAsJSON(os.Stderr))

	router := gin.Default()
	router.GET("/", app.HandlerGin)
	router.GET("/albums", getAlbums)
	router.GET("/attendance/store", app.retrieveAndStoreExpectedAttendance)
	router.GET("/attendance", app.getExpectedAttendance)
	router.GET("/branches/store", app.retrieveAndStoreBranchData)
	router.GET("/branches", app.getBranchData)
	app.Server.Handler = router

	return app, nil
}

const westendName = "westend"
const miltonName = "milton"
const newsteadName = "newstead"

const dataUrl = "https://portal.urbanclimb.com.au/uc-services/ajax/gym/occupancy.ashx?branch="
const expectedUrl = "https://api-prod.urbanclimb.com.au/widgets/trendline-data?branch="

func (a *App) getBranchIds() map[string]string {
	return map[string]string{
		westendName:  "D969F1B2-0C9F-49A9-B2AC-D7775642F298",
		miltonName:   "690326F9-98CE-4249-BD91-53A0676A137B",
		newsteadName: "A3010228-DFC6-4317-86C0-3839FFDF3FD0",
	}
}

func (a *App) getBranchSQLIds() map[string]int {
	return map[string]int{
		westendName:  0,
		miltonName:   1,
		newsteadName: 2,
	}
}

func (a *App) getBranchSQLNames() map[int]string {
	return map[int]string{
		0: westendName,
		1: miltonName,
		2: newsteadName,
	}
}

func (a *App) getDatabase() {
	db, err := connectWithConnector()
	if err != nil {
		log.Println(err)
	}
	a.db = db
}

func (a *App) retrieveAndStoreBranchData(context *gin.Context) {
	var err error
	for name, id := range a.getBranchIds() {
		data := branchData{}
		r, err := http.Get(fmt.Sprintf("%s%s", dataUrl, id))
		if err != nil {
			log.Println(err)
		}
		json.NewDecoder(r.Body).Decode(&data)

		qry := fmt.Sprintf("INSERT INTO `branch-data`.`branch_data`(`branch-id`, `last-updated`, `name`, `status`, `current-percentage`) VALUES ('%s', '%s', '%s', '%s', '%s')",
			strconv.Itoa(a.getBranchSQLIds()[name]),
			data.LastUpdated.Add(10*time.Hour).Format("2006-01-02 15:04:05"),
			data.Name,
			data.Status,
			strconv.FormatFloat(data.CurrentPercentage, 'f', -1, 64))
		_, err = a.db.Query(qry)
		if err != nil {
			log.Println(err)
		}

		r.Body.Close()
	}
	if err != nil {
		context.IndentedJSON(http.StatusInternalServerError, err)
	} else {
		context.IndentedJSON(http.StatusOK, "Store Succeeded")
	}
}

// this is expected to be run once at the start of the day
func (a *App) retrieveAndStoreExpectedAttendance(context *gin.Context) {
	var err error
	// delete all existing data
	deleteQuery := "DELETE FROM `branch-data`.`expected_attendance`"
	_, err = a.db.Query(deleteQuery)
	if err != nil {
		log.Println(err)
	}
	deleteQuery = "DELETE FROM `branch-data`.`branch_data`"
	_, err = a.db.Query(deleteQuery)
	if err != nil {
		log.Println(err)
	}

	// for each branch
	for name, id := range a.getBranchIds() {
		// get the expected attendance for UC
		// could theoretically do this each time the trend is requested, but this feels more polite
		data := make([]expectedAttendance, 16)
		r, err := http.Get(fmt.Sprintf("%s%s", expectedUrl, id))
		if err != nil {
			log.Println(err)
		}
		json.NewDecoder(r.Body).Decode(&data)

		for _, hour := range data {
			qry := fmt.Sprintf("INSERT INTO `branch-data`.`expected_attendance`(`branch-id`, `hour`, `percentage`) VALUES ('%s', '%s', '%s')",
				strconv.Itoa(a.getBranchSQLIds()[name]),
				strconv.Itoa(hour.Hour),
				strconv.FormatFloat(hour.Percentage, 'f', -1, 64))
			_, err = a.db.Query(qry)
			if err != nil {
				log.Println(err)
			}
		}
		r.Body.Close()
	}
	if err != nil {
		context.IndentedJSON(http.StatusInternalServerError, err)
	} else {
		context.IndentedJSON(http.StatusOK, "Store Succeeded")
	}
}

func (a *App) getBranchData(context *gin.Context) {
	getQuery := "SELECT * FROM `branch-data`.branch_data"
	rows, err := a.db.Query(getQuery)
	if err != nil {
		log.Println(err)
	}
	defer rows.Close()
	data := map[string][]branchData{
		westendName:  make([]branchData, 0),
		miltonName:   make([]branchData, 0),
		newsteadName: make([]branchData, 0),
	}
	idMap := a.getBranchSQLNames()
	for rows.Next() {
		var bd branchData
		var id, branchId int
		err = rows.Scan(&id, &branchId, &bd.LastUpdated, &bd.Name, &bd.Status, &bd.CurrentPercentage)
		if err != nil {
			context.IndentedJSON(http.StatusInternalServerError, err)
		}
		appended := append(data[idMap[branchId]], bd)
		data[idMap[branchId]] = appended
	}
	context.IndentedJSON(http.StatusOK, data)
}

func (a *App) getExpectedAttendance(context *gin.Context) {
	getQuery := "SELECT * FROM `branch-data`.expected_attendance"
	rows, err := a.db.Query(getQuery)
	if err != nil {
		log.Println(err)
	}
	defer rows.Close()
	data := map[string][]expectedAttendance{
		westendName:  make([]expectedAttendance, 0),
		miltonName:   make([]expectedAttendance, 0),
		newsteadName: make([]expectedAttendance, 0),
	}
	idMap := a.getBranchSQLNames()
	for rows.Next() {
		var ea expectedAttendance
		var id, branchId int
		err = rows.Scan(&id, &branchId, &ea.Hour, &ea.Percentage)
		if err != nil {
			context.IndentedJSON(http.StatusInternalServerError, err)
		}
		appended := append(data[idMap[branchId]], ea)
		data[idMap[branchId]] = appended
	}
	context.IndentedJSON(http.StatusOK, data)
}

type album struct {
	ID     string  `json:"id"`
	Title  string  `json:"title"`
	Artist string  `json:"artist"`
	Price  float64 `json:"price"`
}

// albums slice to seed record album data.
var albums = []album{
	{ID: "1", Title: "Blue Train", Artist: "John Coltrane", Price: 56.99},
	{ID: "2", Title: "Jeru", Artist: "Gerry Mulligan", Price: 17.99},
	{ID: "3", Title: "Sarah Vaughan and Clifford Brown", Artist: "Sarah Vaughan", Price: 39.99},
}

// getAlbums responds with the list of all albums as JSON.
func getAlbums(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, albums)
}

type branchData struct {
	LastUpdated       time.Time `json:"LastUpdated"`
	Name              string    `json:"Name"`
	Status            string    `json:"Status"`
	CurrentPercentage float64   `json:"CurrentPercentage"`
}

type expectedAttendance struct {
	Hour       int     `json:"hour"`
	Percentage float64 `json:"percantage"`
}

func connectWithConnector() (*sql.DB, error) {
	mustGetenv := func(k string) string {
		v := os.Getenv(k)
		if v == "" {
			log.Fatalf("Fatal Error in connect_connector.go: %s environment variable not set.", k)
		}
		return v
	}
	var (
		dbUser                 = mustGetenv("DB_USER")                  // e.g. 'my-db-user'
		dbPwd                  = mustGetenv("DB_PASS")                  // e.g. 'my-db-password'
		dbName                 = mustGetenv("DB_NAME")                  // e.g. 'my-database'
		instanceConnectionName = mustGetenv("INSTANCE_CONNECTION_NAME") // e.g. 'project:region:instance'
		usePrivate             = os.Getenv("PRIVATE_IP")
	)

	d, err := cloudsqlconn.NewDialer(context.Background())
	if err != nil {
		return nil, fmt.Errorf("cloudsqlconn.NewDialer: %w", err)
	}
	var opts []cloudsqlconn.DialOption
	if usePrivate != "" {
		opts = append(opts, cloudsqlconn.WithPrivateIP())
	}
	mysql.RegisterDialContext("cloudsqlconn",
		func(ctx context.Context, addr string) (net.Conn, error) {
			return d.Dial(ctx, instanceConnectionName, opts...)
		})

	dbURI := fmt.Sprintf("%s:%s@cloudsqlconn(localhost:3306)/%s?parseTime=true",
		dbUser, dbPwd, dbName)

	dbPool, err := sql.Open("mysql", dbURI)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}
	return dbPool, nil
}
