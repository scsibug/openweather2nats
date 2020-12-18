package main

import (
	"encoding/json"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	uuid "github.com/google/uuid"
	nats "github.com/nats-io/nats.go"
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

// Base URL for OpenWeather API ("one-call"), excluding forecasts/alerts
const openWeatherUrlPrefix = "https://api.openweathermap.org/data/2.5/onecall?exclude=minutely,hourly,daily,alerts&"

// getWeather returns a CloudEvent bytearray for the given openweather url
// (zip is passed through).
func getWeather(url string, zipCode string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalln(err)
		// handle error
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	var dat map[string]interface{}
	// parse JSON
	if err := json.Unmarshal(body, &dat); err != nil {
		panic(err)
	}
	// transform openWeather schema to our own Weather struct
	weather := openWeatherTransform(dat, zipCode)
	ce := openWeatherToCloudEvent(weather)
	return ce, nil
}

func addFloatFromKey(w *map[string]interface{}, api *map[string]interface{}, apikey string, weatherkey string, scale float64) {
	v, ok := (*api)[apikey].(float64)
	if ok {
		(*w)[weatherkey] = v * scale
	}
}

// openWeatherTransform converts JSON structure to our own Weather format
func openWeatherTransform(wj map[string]interface{}, zipCode string) []byte {
	// Create a JSON structure matching our weather json schema
	// Create the empty weather struct
	w := make(map[string]interface{})
	// Get main object
	main, _ := wj["current"].(map[string]interface{})
	w["dt"] = main["dt"].(float64)
	// Temperature
	addFloatFromKey(&w, &main, "temp", "temp", 1.0)
	fmt.Printf("Current temp is %.2f F\n", w["temp"].(float64)*(9.0/5.0)-459.67)
	addFloatFromKey(&w, &main, "feels_like", "feels_like", 1.0)
	addFloatFromKey(&w, &main, "uvi", "uvi", 1.0)
	addFloatFromKey(&w, &main, "pressure", "pressure", 1.0)
	addFloatFromKey(&w, &main, "humidity", "humidity", (1 / 100.0))
	addFloatFromKey(&w, &main, "sunrise", "sunrise", 1.0)
	addFloatFromKey(&w, &main, "sunset", "sunset", 1.0)
	addFloatFromKey(&w, &main, "clouds", "clouds", (1 / 100.0))
	addFloatFromKey(&w, &main, "wind_deg", "wind_deg", 1.0)
	addFloatFromKey(&w, &main, "wind_speed", "wind_speed", 1.0)
	addFloatFromKey(&w, &main, "dew_point", "dew_point", 1.0)
	addFloatFromKey(&w, &main, "visibility", "visibility", 1.0)
	// Get location
	l := make(map[string]interface{})
	l["lat"] = wj["lat"].(float64)
	l["lon"] = wj["lon"].(float64)
	l["zip"] = zipCode
	w["loc"] = l
	// Get weather description object
	desc, _ := main["weather"].([]interface{})
	descriptions := make([]string, 0)
	for _, v := range desc {
		val := v.(map[string]interface{})
		descriptions = append(descriptions, val["description"].(string))
	}
	w["descriptions"] = descriptions
	// Rain amount
	rain, ok := main["rain"].(map[string]interface{})
	r := make(map[string]interface{})
	if ok {
		r["1h"] = rain["1h"].(float64)
		r["3h"] = rain["3h"].(float64)
	} else {
		r["1h"] = 0.0
		r["3h"] = 0.0
	}
	w["rain"] = r
	// Snow amount
	snow, ok := main["snow"].(map[string]interface{})
	s := make(map[string]interface{})
	if ok {
		s["1h"] = snow["1h"].(float64)
		s["3h"] = snow["3h"].(float64)
	} else {
		s["1h"] = 0.0
		s["3h"] = 0.0
	}
	w["snow"] = s
	wb, err := json.Marshal(w)
	if err != nil {
		panic(err)
	}
	return wb
}

func openWeatherToCloudEvent(data []byte) []byte {
	id := uuid.Must(uuid.NewRandom())
	event := cloudevents.NewEvent()
	event.SetID(id.String())
	event.SetSource("https://openweathermap.org/")
	event.SetType("com.wellorder.iot.weather")
	event.SetData(cloudevents.ApplicationJSON, string(data))
	bytes, err := json.Marshal(event)
	if err != nil {
		panic(err)
	}
	//fmt.Println("DATA:  " + string(data))
	return bytes
}

type Config struct {
	NATS      string `yaml:"natsServer"`
	NATSTOPIC string `yaml:"natsTopic"`
	APIKEY    string `yaml:"apikey"`
	LAT       string `yaml:"lat"`
	LON       string `yaml:"lon"`
	ZIP       string `yaml:"zipcode"`
}

func readConfig(filename string) (cfg Config) {
	// Read application config
	f, err := os.Open("config.yml")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&cfg)
	if err != nil {
		panic(err)
	}
	return
}

func getAndPublish(apiurl string, zip string, nats *nats.Conn, topic string) {
	w, werr := getWeather(apiurl, zip)
	if werr == nil {
		//fmt.Println(string(w))
		nats.Publish(topic, w)
	}
}

func main() {
	cfg := readConfig("config.yml")

	// Get config items ready
	openWeatherApiUrl := openWeatherUrlPrefix +
		"lat=" + cfg.LAT +
		"&lon=" + cfg.LON +
		"&APPID=" + cfg.APIKEY
	natsServer := cfg.NATS
	zipCode := cfg.ZIP

	// Topic for publishing events
	natsTopic := "iot.weather"
	if nt := cfg.NATSTOPIC; nt != "" {
		natsTopic = nt
	}

	// Connect to NATS server
	nc, err := nats.Connect(natsServer)
	defer nc.Close()
	if err != nil {
		log.Fatalf("Could not instantiate NATS client: %v", err)
		panic(err)
	}
	// Get new event every 2 minutes
	for {
		go getAndPublish(openWeatherApiUrl, zipCode, nc, natsTopic)
		time.Sleep(120 * time.Second)
	}
}
