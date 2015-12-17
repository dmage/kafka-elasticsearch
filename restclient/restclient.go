package restclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"

	log "github.com/Sirupsen/logrus"
)

type Client struct {
	URL    string
	Client http.Client
	Header http.Header
}

type ResponseError struct {
	URL    string
	Status string
	Err    error
}

func (e ResponseError) Error() string {
	return fmt.Sprintf("%s from %s: %s", e.Status, e.URL, e.Err)
}

type Response struct {
	url string
	*http.Response
}

func (r *Response) Decode(v interface{}) error {
	err := json.NewDecoder(r.Body).Decode(v)
	if err != nil {
		return &ResponseError{
			Status: r.Status,
			URL:    r.url,
			Err:    fmt.Errorf("parse response: %s", err),
		}
	}
	return nil
}

func (r *Response) Close() error {
	return r.Body.Close()
}

func readHead(reader io.ReadCloser, limit int64) (head []byte, all bool, err error) {
	defer func() {
		if cerr := reader.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	head, err = ioutil.ReadAll(&io.LimitedReader{R: reader, N: limit})
	if err != nil {
		return
	}

	all = true
	_, err = reader.Read(make([]byte, 1))
	if err == io.EOF {
		err = nil
	} else {
		all = false
	}
	return
}

func (c *Client) Get(path string, params url.Values) (resp *Response, err error) {
	url := c.URL + path
	if params != nil {
		url += "?" + params.Encode()
	}

	log.Debug("restclient: GET ", url, "...")

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header = c.Header

	httpResp, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}

	mediatype, _, err := mime.ParseMediaType(httpResp.Header.Get("Content-Type"))
	if err != nil {
		mediatype = ""
	}

	if mediatype != "application/json" {
		head, all, err := readHead(httpResp.Body, 512)
		if err != nil {
			return nil, &ResponseError{
				Status: httpResp.Status,
				URL:    url,
				Err:    fmt.Errorf("read error: %s", err),
			}
		}

		suffix := ""
		if !all {
			suffix = "..."
		}

		return nil, &ResponseError{
			Status: httpResp.Status,
			URL:    url,
			Err:    fmt.Errorf("got response with content-type %s: %s%s", mediatype, head, suffix),
		}
	}

	return &Response{
		url:      url,
		Response: httpResp,
	}, nil
}

func (c *Client) Put(path string, data interface{}, result, failure interface{}) (resp *http.Response, err error) {
	url := c.URL + path

	log.Debug("restclient: PUT ", url, "...")

	var buf []byte
	if data != nil {
		buf, err = json.Marshal(data)
		if err != nil {
			return nil, err
		}
	}

	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	req.Header = c.Header

	resp, err = c.Client.Do(req)
	if err != nil {
		return resp, err
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	mediatype, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		mediatype = ""
	}

	if mediatype != "application/json" {
		head, err := ioutil.ReadAll(&io.LimitedReader{R: resp.Body, N: 512})
		if err != nil {
			return resp, fmt.Errorf("read non-json %s response from %s: %s", resp.Status, url, err)
		}

		suffix := ""
		_, err = resp.Body.Read(make([]byte, 1))
		if err != io.EOF {
			suffix = "..."
		}
		return resp, fmt.Errorf("got non-json %s response from %s: %s%s", resp.Status, url, head, suffix)
	}

	if resp.StatusCode < 300 {
		err = json.NewDecoder(resp.Body).Decode(&result)
		if err != nil {
			return resp, fmt.Errorf("parse %s response from %s: %s", resp.Status, url, err.Error())
		}
	} else if resp.StatusCode >= 400 {
		err = json.NewDecoder(resp.Body).Decode(&failure)
		if err != nil {
			return resp, fmt.Errorf("parse %s response from %s: %s", resp.Status, url, err.Error())
		}
	}
	return
}
