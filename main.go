package main

import (
	"encoding/json"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	uuid "github.com/google/uuid"
	nats "github.com/nats-io/nats.go"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

type Location struct {
	Lon float64 `json:"lon"`
	Lat float64 `json:"lat"`
	Zip string  `json:"zip"`
}

type RainVolume struct {
	OneHour   float64 `json:"rain.1h",omitempty`
	ThreeHour float64 `json:"rain.3h",omitempty`
}

type SnowVolume struct {
	OneHour   float64 `json:"snow.1h",omitempty`
	ThreeHour float64 `json:"snow.3h",omitempty`
}

type Weather struct {
	Location    Location   `json:"location"`
	Description []string   `json:"description",omitempty`
	Temp        float64    `json:"temp"`
	FeelsLike   float64    `json:"feels_like",omitempty`
	DewPoint    float64    `json:"dew_point",omitempty`
	UVIndex     float64    `json:"uvi",omitempty`
	Pressure    float64    `json:"pressure",omitempty`
	Humidity    float64    `json:"humidity",omitempty`
	Clouds      float64    `json:"clouds",omitempty`
	WindSpeed   float64    `json:"windspeed",omitempty`
	WindDegree  float64    `json:"winddeg",omitempty`
	Visibility  float64    `json:"visibility",omitempty`
	Sunrise     float64    `json:"sunrise",omitempty`
	Sunset      float64    `json:"sunset",omitempty`
	Rain        RainVolume `json:"rain",omitempty`
	Snow        SnowVolume `json:"snow",omitempty`
}

func getWeather(url string) ([]byte, error) {
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
	weather := openWeatherTransform(dat)
	// create JSON
	wj, err := json.Marshal(weather)
	if err != nil {
		panic(err)
	}
	ce := openWeatherToCloudEvent(wj)
	//	fmt.Println(string(body))
	//	fmt.Println(string(ce))
	return ce, nil
}

func openWeatherTransform(wj map[string]interface{}) Weather {
	// Create a JSON structure matching our weather json schema
	// Create the empty weather struct
	var w Weather
	// Get main object
	//fmt.Println(wj)

	// current:map[clouds:1 dew_point:271.86 dt:1.608169023e+09 feels_like:273.25 humidity:74 pressure:1021 sunrise:1.608125108e+09 sunset:1.60816106e+09 temp:275.85 uvi:0 visibility:10000

	main, _ := wj["current"].(map[string]interface{})
	w.Temp = main["temp"].(float64)
	w.FeelsLike = main["feels_like"].(float64)
	w.UVIndex = main["uvi"].(float64)
	w.Pressure = main["pressure"].(float64)
	w.Humidity = main["humidity"].(float64) / 100.0
	w.Sunrise = main["sunrise"].(float64)
	w.Sunset = main["sunset"].(float64)
	w.Visibility = main["visibility"].(float64)
	// Weather description object
	desc, ok := main["weather"].([]interface{})
	fmt.Printf("%T\n", main["weather"])
	if !ok {
		fmt.Println("Did not get weather object")
	}
	descriptions := make([]string, 0)
	for _, v := range desc {
		val := v.(map[string]interface{})
		// Gather each "description" key from the array
		fmt.Println(val["description"])
		descriptions = append(descriptions, val["description"].(string))
	}
	w.Description = descriptions
	fmt.Println(w)
	return w
}

func openWeatherToCloudEvent(data []byte) []byte {
	id := uuid.Must(uuid.NewRandom())
	event := cloudevents.NewEvent()
	event.SetID(id.String())
	event.SetSource("com.wellorder.iot.weather")
	event.SetType("https://openweathermap.org/")
	event.SetData(cloudevents.ApplicationJSON, string(data))
	bytes, err := json.Marshal(event)
	if err != nil {
		fmt.Println("We got an error: ")
		fmt.Println(err)
	}
	fmt.Println("DATA:  " + string(data))
	return bytes
}

func main() {
	openWeatherApiUrl := os.Getenv("OPENWEATHER_URL")
	// todo err if no api/url defined
	natsServer := os.Getenv("NATS_SERVER")
	// todo err if no server defined
	natsTopic := "iot.weather"
	if nt := os.Getenv("NATS_TOPIC"); nt != "" {
		natsTopic = nt
	}

	nc, err := nats.Connect(natsServer)
	defer nc.Close()
	if err != nil {
		log.Fatalf("Could not instantiate NATS client: %v", err)
	}

	for {
		w, werr := getWeather(openWeatherApiUrl)
		fmt.Println(string(w))
		if werr == nil {
			fmt.Println("Publishing")
			nc.Publish(natsTopic, w)
		}
		time.Sleep(120 * time.Second)
	}
	if err != nil {
		log.Fatal(err)
	}

}
