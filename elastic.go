package elastic

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"time"

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

type ElasticQueryMust struct {
	Query struct {
		Bool struct {
			Must []interface{} `json:"must"`
		} `json:"bool"`
	} `json:"query"`
	Size     int     `json:"size"`
	From     int     `json:"from"`
	MinScore float32 `json:"min_score"`
}

type ElasticQueryMustShould struct {
	Query struct {
		Bool struct {
			Must   []interface{} `json:"must"`
			Should []interface{} `json:"should"`
		} `json:"bool"`
	} `json:"query"`
	Size     int     `json:"size"`
	From     int     `json:"from"`
	MinScore float32 `json:"min_score"`
}

type ElasticQueryMustWithSort struct {
	Query struct {
		Bool struct {
			Must []interface{} `json:"must"`
		} `json:"bool"`
	} `json:"query"`
	Size     int           `json:"size"`
	From     int           `json:"from"`
	MinScore float32       `json:"min_score"`
	Sort     []interface{} `json:"sort"`
}

type ElasticQueryWithSortShould struct {
	ElasticQueryMustShould

	Sort []interface{} `json:"sort"`
}

type ElasticQueryWithSort struct {
	ElasticQuery

	Sort []interface{} `json:"sort"`
}

type ElasticQueryWithShould struct {
	Query struct {
		Bool struct {
			Must   []interface{} `json:"must"`
			Should []interface{} `json:"should"`
		} `json:"bool"`
	} `json:"query"`
	Size int           `json:"size"`
	From int           `json:"from"`
	Sort []interface{} `json:"sort"`
}

type ElasticQueryWithMust struct {
	Query struct {
		Bool struct {
			Must []interface{} `json:"must"`
		} `json:"bool"`
	} `json:"query"`
	Size int           `json:"size"`
	From int           `json:"from"`
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

type ElasticHitsMGet struct {
	Index   string `json:"_index"`
	Type    string `json:"_type"`
	ID      string `json:"_id"`
	Version int    `json:"_version"`
	Found   bool   `json:"found"`
	Source  struct {
		Data interface{} `json:"data"`
	} `json:"_source"`
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

type ElasticResponseMGet struct {
	Docs []ElasticHitsMGet `json:"docs"`
}

type Elastic struct {
	BaseURL string
	Index   string
	Type    string
}

func (e *Elastic) GetDataFromId(c context.Context, id string) (ElasticHitsMGet, error) {
	contextWithTimeout, _ := context.WithTimeout(c, time.Second*15)
	client := urlfetch.Client(contextWithTimeout)
	getUrl := e.BaseURL + "/" + e.Index + "/" + id

	req, _ := http.NewRequest("GET", getUrl, nil)
	if os.Getenv("ELASTIC_PASS") != "" && os.Getenv("ELASTIC_PASS") != "" {
		req.SetBasicAuth(os.Getenv("ELASTIC_USER"), os.Getenv("ELASTIC_PASS"))
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Errorf(c, "%v", err)
		return ElasticHitsMGet{}, err
	}
	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)
	var elasticResponse ElasticHitsMGet
	err = decoder.Decode(&elasticResponse)
	if err != nil {
		log.Errorf(c, "%v", err)
		return ElasticHitsMGet{}, err
	}

	return elasticResponse, nil
}

func (e *Elastic) Query(c context.Context, offset int, limit int, searchQuery string) (ElasticHits, error) {
	contextWithTimeout, _ := context.WithTimeout(c, time.Second*15)
	client := urlfetch.Client(contextWithTimeout)
	getUrl := e.BaseURL + "/" + e.Index + "/" + e.Type + "/_search?size=" + strconv.Itoa(limit) + "&from=" + strconv.Itoa(offset) + "&" + searchQuery

	req, _ := http.NewRequest("GET", getUrl, nil)
	if os.Getenv("ELASTIC_PASS") != "" && os.Getenv("ELASTIC_PASS") != "" {
		req.SetBasicAuth(os.Getenv("ELASTIC_USER"), os.Getenv("ELASTIC_PASS"))
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Errorf(c, "%v", err)
		return ElasticHits{}, err
	}
	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)
	var elasticResponse ElasticResponse
	err = decoder.Decode(&elasticResponse)
	if err != nil {
		log.Errorf(c, "%v", err)
		return ElasticHits{}, err
	}

	return elasticResponse.Hits, nil
}

func (e *Elastic) GetMapping(c context.Context) (interface{}, error) {
	contextWithTimeout, _ := context.WithTimeout(c, time.Second*15)
	client := urlfetch.Client(contextWithTimeout)
	getUrl := e.BaseURL + "/" + e.Index + "/" + e.Type + "/_mapping"

	req, _ := http.NewRequest("GET", getUrl, nil)
	if os.Getenv("ELASTIC_PASS") != "" && os.Getenv("ELASTIC_PASS") != "" {
		req.SetBasicAuth(os.Getenv("ELASTIC_USER"), os.Getenv("ELASTIC_PASS"))
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Errorf(c, "%v", err)
		return nil, err
	}
	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)
	var elasticResponse interface{}
	err = decoder.Decode(&elasticResponse)
	if err != nil {
		log.Errorf(c, "%v", err)
		return nil, err
	}

	return elasticResponse, nil
}

func (e *Elastic) performQuery(c context.Context, readerQuery *bytes.Reader, searchQuery string) (ElasticHits, error) {
	contextWithTimeout, _ := context.WithTimeout(c, time.Second*30)
	client := urlfetch.Client(contextWithTimeout)
	getUrl := e.BaseURL + "/" + e.Index + "/" + e.Type + "/_search"

	if searchQuery != "" {
		getUrl += "?" + searchQuery
	}

	req, _ := http.NewRequest("POST", getUrl, readerQuery)
	if os.Getenv("ELASTIC_PASS") != "" && os.Getenv("ELASTIC_PASS") != "" {
		req.SetBasicAuth(os.Getenv("ELASTIC_USER"), os.Getenv("ELASTIC_PASS"))
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Errorf(c, "%v", err)
		return ElasticHits{}, err
	}
	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)
	var elasticResponse ElasticResponse
	err = decoder.Decode(&elasticResponse)
	if err != nil {
		log.Errorf(c, "%v", err)
		return ElasticHits{}, err
	}

	return elasticResponse.Hits, nil
}

func (e *Elastic) performMGetQuery(c context.Context, readerQuery *bytes.Reader) ([]ElasticHitsMGet, error) {
	contextWithTimeout, _ := context.WithTimeout(c, time.Second*30)
	client := urlfetch.Client(contextWithTimeout)
	getUrl := e.BaseURL + "/" + e.Index + "/" + e.Type + "/_mget"

	req, _ := http.NewRequest("POST", getUrl, readerQuery)
	if os.Getenv("ELASTIC_PASS") != "" && os.Getenv("ELASTIC_PASS") != "" {
		req.SetBasicAuth(os.Getenv("ELASTIC_USER"), os.Getenv("ELASTIC_PASS"))
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Errorf(c, "%v", err)
		return []ElasticHitsMGet{}, err
	}
	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)
	var elasticResponse ElasticResponseMGet
	err = decoder.Decode(&elasticResponse)
	if err != nil {
		log.Errorf(c, "%v", err)
		return []ElasticHitsMGet{}, err
	}

	return elasticResponse.Docs, nil
}

func (e *Elastic) QueryStruct(c context.Context, searchQuery interface{}) (ElasticHits, error) {
	SearchQuery, err := json.Marshal(searchQuery)
	if err != nil {
		log.Errorf(c, "%v", err)
		return ElasticHits{}, err
	}
	log.Infof(c, "%v", string(SearchQuery))
	readerQuery := bytes.NewReader(SearchQuery)
	return e.performQuery(c, readerQuery, "")
}

func (e *Elastic) QueryStructWithSearchQueryUrl(c context.Context, searchQuery interface{}, searchQueryUrl string) (ElasticHits, error) {
	SearchQuery, err := json.Marshal(searchQuery)
	if err != nil {
		log.Errorf(c, "%v", err)
		return ElasticHits{}, err
	}
	log.Infof(c, "%v", string(SearchQuery))
	readerQuery := bytes.NewReader(SearchQuery)
	return e.performQuery(c, readerQuery, searchQueryUrl)
}

func (e *Elastic) QueryStructMGet(c context.Context, searchQuery interface{}) ([]ElasticHitsMGet, error) {
	SearchQuery, err := json.Marshal(searchQuery)
	if err != nil {
		log.Errorf(c, "%v", err)
		return []ElasticHitsMGet{}, err
	}
	log.Infof(c, "%v", string(SearchQuery))
	readerQuery := bytes.NewReader(SearchQuery)
	return e.performMGetQuery(c, readerQuery)
}
