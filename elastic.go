package elastic

import (
	"encoding/json"
	"errors"
	"strconv"

	"golang.org/x/net/context"

	"google.golang.org/appengine/urlfetch"
)

type ElasticResponse struct {
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
	Shards   struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
	Hits struct {
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
	} `json:"hits"`
}

type Elastic struct {
	BaseURL string
}

func (e *Elastic) Query(c context.Context, resourceType string, offset int, limit int) (interface{}, error) {
	client := urlfetch.Client(c)
	resp, err := client.Get(e.BaseURL + "/" + resourceType + "/_search?size=" + strconv.Itoa(limit) + "&from=" + strconv.Itoa(offset) + "&q=data.Name:" + search)
	if err != nil {
		log.Errorf(c, "%v", err)
		return nil, err
	}

	decoder := json.NewDecoder(resp.Body)
	var elasticResponse ElasticResponse
	err = decoder.Decode(&elasticResponse)
	if err != nil {
		log.Errorf(c, "%v", err)
		return nil, err
	}

	return elasticResponse.Hits.Hits, nil
}
