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
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"cloud.google.com/go/logging"
	"example.com/micro/metadata"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type App struct {
	*http.Server
	projectID string
	log       *logging.Logger
	data      map[string][]branchData
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
	app.cleanBranchMap()

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

	// Setup request router.
	/*r := mux.NewRouter()
	r.HandleFunc("/", app.Handler).
		Methods("GET")
	app.Server.Handler = r*/

	router := gin.Default()
	router.GET("/", app.HandlerGin)
	router.GET("/albums", getAlbums)
	router.GET("/branches", app.getAllBranches)
	app.Server.Handler = router

	return app, nil
}

const westendName = "westend"
const miltonName = "milton"
const newsteadName = "newstead"

const url = "https://portal.urbanclimb.com.au/uc-services/ajax/gym/occupancy.ashx?branch="

func (a *App) getBranchIds() map[string]string {
	return map[string]string{
		westendName:  "D969F1B2-0C9F-49A9-B2AC-D7775642F298",
		miltonName:   "690326F9-98CE-4249-BD91-53A0676A137B",
		newsteadName: "A3010228-DFC6-4317-86C0-3839FFDF3FD0",
	}
}

func (a *App) cleanBranchMap() {
	a.data = map[string][]branchData{
		westendName:  make([]branchData, 0),
		miltonName:   make([]branchData, 0),
		newsteadName: make([]branchData, 0),
	}
}

func (a *App) getAllBranches(context *gin.Context) {
	for name, id := range a.getBranchIds() {
		data := branchData{}
		r, err := http.Get(fmt.Sprintf("%s%s", url, id))
		if err != nil {
			log.Println(err)
		}
		defer r.Body.Close()

		json.NewDecoder(r.Body).Decode(&data)
		i := append(a.data[name], data)
		a.data[name] = i
	}

	context.IndentedJSON(http.StatusOK, a.data)
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
	LastUpdated       time.Time     `json:"LastUpdated"`
	Name              string        `json:"Name"`
	Current           int           `json:"Current"`
	Leaving           int           `json:"Leaving"`
	Allowed           int           `json:"Allowed"`
	Bookings          int           `json:"Bookings"`
	Capacity          int           `json:"Capacity"`
	Offset            int           `json:"Offset"`
	GoogleStatus      string        `json:"GoogleStatus"`
	Remaining         int           `json:"Remaining"`
	Status            string        `json:"Status"`
	Colour            string        `json:"Colour"`
	CurrentPercentage float64       `json:"CurrentPercentage"`
	FontColour        string        `json:"FontColour"`
	WaitingTime       string        `json:"WaitingTime"`
	KidsNames         []interface{} `json:"KidsNames"`
}
