package main

import (
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"
	"io/ioutil"
    "github.com/dghubble/go-twitter/twitter"
    "github.com/dghubble/oauth1"
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/jmoiron/sqlx"
	"time"
	"net/http"
	"encoding/json"
	"github.com/gorilla/mux"
	"gopkg.in/guregu/null.v3"
)

const consumerKey = "dRY18aJqRepwWfpYGd2DvrWYW";
const accessToken =  "716162453751640065-nvDNlhvDPTSuhezuYgOnFcdirxO51a1"

const (
	DB_USER     = "gouser"
	DB_PASSWORD = "gouser"
	DB_NAME     = "twitter_go"
)

func configure() {

	consumerSecret := getFileContent("files/consumer-secret.txt")
	accessSecret := getFileContent("files/access-secret.txt")
    // Pass in your consumer key (API Key) and your Consumer Secret (API Secret)
    config := oauth1.NewConfig(consumerKey, consumerSecret)
    // Pass in your Access Token and your Access Token Secret
    token := oauth1.NewToken(accessToken, accessSecret)
    httpClient := config.Client(oauth1.NoContext, token)
    client := twitter.NewClient(httpClient)

	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
		DB_USER, DB_PASSWORD, DB_NAME)
	db, err := sql.Open("postgres", dbinfo)
	check(err)
	defer db.Close()

    demux := twitter.NewSwitchDemux()

    demux.Tweet = func(tweet *twitter.Tweet){
		lastInsertId := 0
		if tweet.Coordinates != nil {
			err = db.QueryRow("INSERT INTO tweet(username, text, userlocation, tweetlong, tweetlat, created) " +
				"VALUES($1,$2,$3, $4, $5, $6) returning id", tweet.User.Name, tweet.Text, tweet.User.Location,
				tweet.Coordinates.Coordinates[0], tweet.Coordinates.Coordinates[1],time.Now()).Scan(&lastInsertId)
		} else if tweet.Place != nil {
			long := getFloatAvg(tweet.Place.BoundingBox.Coordinates[0][0][0], tweet.Place.BoundingBox.Coordinates[0][1][0])
			lat := getFloatAvg(tweet.Place.BoundingBox.Coordinates[0][0][1], tweet.Place.BoundingBox.Coordinates[0][2][1])
			err = db.QueryRow("INSERT INTO tweet(username,text,userlocation, placecountry, placename, placelong, placelat, created) " +
				"VALUES($1,$2,$3, $4, $5, $6, $7, $8) returning id", tweet.User.Name, tweet.Text, tweet.User.Location,
				tweet.Place.Country, tweet.Place.Name, long, lat, time.Now()).Scan(&lastInsertId)
			check(err)
		} else if tweet.User != nil {
			err = db.QueryRow("INSERT INTO tweet(created, username,text,userlocation) VALUES($1,$2,$3, $4) returning id",
				time.Now(), tweet.User.Name, tweet.Text, tweet.User.Location).Scan(&lastInsertId)
			check(err)
		}
		for _, tag := range tweet.Entities.Hashtags {
			db.Exec("INSERT INTO hashtags(tweet_id, tag) VALUES ($1, $2)", lastInsertId, tag.Text)
		}
    }

    fmt.Println("Starting Stream...")

    // FILTER
	filterParams := &twitter.StreamFilterParams{
		Track:         []string{"suomi100", "iitti", "cinia", "keskusta", "kokoomus", "perussuomalaiset", "sininentulevaisuus",
		"vihreät", "vasemmistoliitto", "rkp", "sdp", "kristillisdemokraatit", "piraattipuolue", "hallituskriisi", "sote"},
		StallWarnings: twitter.Bool(true),
	}
	stream, err := client.Streams.Filter(filterParams)
	if err != nil {
		log.Fatal(err)
	}

    // Receive messages until stopped or stream quits
	go demux.HandleChan(stream.Messages)

	// Wait for SIGINT and SIGTERM (HIT CTRL-C)
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println(<-ch)

	fmt.Println("Stopping Stream...")
	stream.Stop()

}

func getFileContent(path string) string {
	content, err := ioutil.ReadFile(path)
	check(err)
	stringContent := string(content)
	return stringContent
}

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func getFloatAvg(num1 float64, num2 float64) float64 {
	return (num1 + num2) / 2
}

func Database() *sqlx.DB {
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
		DB_USER, DB_PASSWORD, DB_NAME)

	db, err := sqlx.Connect("postgres", dbinfo)
	check(err)
	return db
}

func main() {
    fmt.Println("Go-Twitter Bot v0.01")
    go configure()
	router := mux.NewRouter()
	router.HandleFunc("/tweets/", Tweets).Methods("GET")
	router.HandleFunc("/tweets/{tag}", TweetsWithTag).Methods("GET")
	http.ListenAndServe(":8080", router)
}

func TweetsWithTag(writer http.ResponseWriter, request *http.Request) {
	params := mux.Vars(request)
	Tweets := []TweetStruct{}
	tag := params["tag"]
	db := Database()
	rows, err := db.NamedQuery(`SELECT * FROM tweet WHERE id IN (SELECT tweet_id FROM hashtags WHERE tweet_id = id AND tag = :tag) ORDER BY id desc LIMIT 5000`,  map[string]interface{}{"tag": tag})
	check(err)

	err = sqlx.StructScan(rows, &Tweets)
	check(err)

	response, err := json.Marshal(Tweets)
	check(err)
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	writer.Write(response)

}

func Tweets (w http.ResponseWriter, r *http.Request) {
	Tweets := []TweetStruct{}
	db := Database()
	db.Select(&Tweets, "SELECT * FROM tweet ORDER BY id desc LIMIT 5000")

	response, err := json.Marshal(Tweets)
	check(err)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

type TweetStruct struct {
	Id				null.Int	`json:"id"`
	Text			null.String	`json:"text"`
	UserLocation	null.String	`json:"userLocation"`
	PlaceCountry	null.String	`json:"placeCountry"`
	PlaceName		null.String	`json:"placeName"`
	PlaceLong		null.Float	`json:"placeLong"`
	PlaceLat		null.Float	`json:"placeLat"`
	UserName		null.String	`json:"userName"`
	TweetLong 		null.Float	`json:"tweetLong"`
	TweetLat 		null.Float	`json:"tweetLat"`
	Created 		null.Time		`json:"created"`
}
