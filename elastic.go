package elastic

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"strconv"

	"golang.org/x/net/context"

	"google.golang.org/appengine/log"
	"google.golang.org/appengine/urlfetch"
)

type ElasticMatchQuery struct {
	Match struct {
		All string `json:"_all"`
	} `json:"match"`
}

type ElasticQuery struct {
	Query struct {
		Bool struct {
			Must []interface{} `json:"must"`
		} `json:"bool"`
	} `json:"query"`
	Size int `json:"size"`
	From int `json:"from"`
}

type ElasticQueryWithSort struct {
	ElasticQuery

	Sort []interface{} `json:"sort"`
}

type ElasticFilterWithSort struct {
	Query struct {
		Bool struct {
			Should             []interface{} `json:"should"`
			MinimumShouldMatch string        `json:"minimum_should_match"`
		} `json:"bool"`
	} `json:"query"`

	Size     int     `json:"size"`
	From     int     `json:"from"`
	MinScore float32 `json:"min_score"`

	Sort []interface{} `json:"sort"`
}

type ElasticHits struct {
	Total    int     `json:"total"`
	MaxScore float64 `json:"max_score"`
	Hits     []struct {
		Index  string  `json:"_index"`
		Type   string  `json:"_type"`
		ID     string  `json:"_id"`
		Score  float64 `json:"_score"`
		Source struct {
			Data interface{} `json:"data"`
		} `json:"_source"`
	} `json:"hits"`
}

type ElasticResponse struct {
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
	Shards   struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
	Hits ElasticHits `json:"hits"`
}

type Elastic struct {
	BaseURL string
	Index   string
	Type    string
}

func (e *Elastic) Query(c context.Context, offset int, limit int, searchQuery string) (ElasticHits, error) {
	client := urlfetch.Client(c)
	getUrl := e.BaseURL + "/" + e.Index + "/_search?size=" + strconv.Itoa(limit) + "&from=" + strconv.Itoa(offset) + "&" + searchQuery

	req, _ := http.NewRequest("GET", getUrl, nil)
	if os.Getenv("ELASTIC_PASS") != "" && os.Getenv("ELASTIC_PASS") != "" {
		req.SetBasicAuth(os.Getenv("ELASTIC_USER"), os.Getenv("ELASTIC_PASS"))
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Errorf(c, "%v", err)
		return ElasticHits{}, err
	}

	decoder := json.NewDecoder(resp.Body)
	var elasticResponse ElasticResponse
	err = decoder.Decode(&elasticResponse)
	if err != nil {
		log.Errorf(c, "%v", err)
		return ElasticHits{}, err
	}

	return elasticResponse.Hits, nil
}

func (e *Elastic) performQuery(c context.Context, readerQuery *bytes.Reader) (ElasticHits, error) {
	client := urlfetch.Client(c)
	getUrl := e.BaseURL + "/" + e.Index + "/" + e.Type + "/_search"

	req, _ := http.NewRequest("POST", getUrl, readerQuery)
	if os.Getenv("ELASTIC_PASS") != "" && os.Getenv("ELASTIC_PASS") != "" {
		req.SetBasicAuth(os.Getenv("ELASTIC_USER"), os.Getenv("ELASTIC_PASS"))
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Errorf(c, "%v", err)
		return ElasticHits{}, err
	}

	decoder := json.NewDecoder(resp.Body)
	var elasticResponse ElasticResponse
	err = decoder.Decode(&elasticResponse)
	if err != nil {
		log.Errorf(c, "%v", err)
		return ElasticHits{}, err
	}

	return elasticResponse.Hits, nil
}

func (e *Elastic) QueryStruct(c context.Context, searchQuery interface{}) (ElasticHits, error) {
	SearchQuery, err := json.Marshal(searchQuery)
	if err != nil {
		log.Errorf(c, "%v", err)
		return ElasticHits{}, err
	}
	log.Infof(c, "%v", string(SearchQuery))
	readerQuery := bytes.NewReader(SearchQuery)
	return e.performQuery(c, readerQuery)
}
