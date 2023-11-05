package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/odit-bit/indexstore/index"
	"github.com/odit-bit/linkstore/linkgraph"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/multierr"
)

var (
	searchEndpoint     = "/search"
	submitLinkEndpoint = "/submit/site"
	indexEndpoint      = "/"
	metricEndpoint     = "/prom"

	defaultResultsPerPage   = 10
	defaultMaxSummaryLength = 256
)

type GraphAPI interface {
	UpsertLink(*linkgraph.Link) error
}

type IndexAPI interface {
	Search(index.Query) (index.Iterator, error)
}

// Config encapsulates the settings for configuring the front-end service.
type Config struct {
	// An API for adding links to the link graph.
	GraphAPI GraphAPI

	// An API for executing queries against indexed documents.
	IndexAPI IndexAPI

	// The port to listen for incoming requests.
	ListenAddr string

	// The number of results to display per page. If not specified, a default
	// value of 10 results per page will be used instead.
	ResultsPerPage int

	// The maximum length (in characters) of the highlighted content summary for
	// matching documents. If not specified, a default value of 256 will be used
	// instead.
	MaxSummaryLength int

	// The logger to use. If not defined an output-discarding logger will
	// be used instead.
	// Logger *logrus.Entry
}

func (cfg *Config) validate() error {
	var err error
	if cfg.ListenAddr == "" {
		err = multierr.Append(err, fmt.Errorf("listen address has not been specified"))
	}
	if cfg.ResultsPerPage <= 0 {
		cfg.ResultsPerPage = defaultResultsPerPage
	}
	if cfg.MaxSummaryLength <= 0 {
		cfg.MaxSummaryLength = defaultMaxSummaryLength
	}
	if cfg.IndexAPI == nil {
		err = multierr.Append(err, fmt.Errorf("index API has not been provided"))
	}
	if cfg.GraphAPI == nil {
		err = multierr.Append(err, fmt.Errorf("graph API has not been provided"))
	}
	// if cfg.Logger == nil {
	// 	cfg.Logger = logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard})
	// }
	return err
}

type API struct {
	router       *chi.Mux
	cfg          Config
	templateFunc func(tpl *template.Template, w io.Writer, data map[string]interface{}) error
}

func NewDefault(graphDB GraphAPI, indexDB IndexAPI) *API {
	//front end setup
	fr, err := new(Config{
		GraphAPI:         graphDB,
		IndexAPI:         indexDB,
		ListenAddr:       ":8080",
		ResultsPerPage:   10,
		MaxSummaryLength: 256,
	})
	if err != nil {
		log.Fatal(err)
	}
	return fr
}

func new(cfg Config) (*API, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	a := API{
		router: chi.NewMux(),
		cfg:    cfg,
		templateFunc: func(tpl *template.Template, w io.Writer, data map[string]interface{}) error {
			return tpl.Execute(w, data)
		},
	}

	a.router.Get(indexEndpoint, a.renderIndexPage)

	a.router.Get(searchEndpoint, a.renderSearchResults)

	a.router.Get(submitLinkEndpoint, a.submitLink)

	a.router.Post(submitLinkEndpoint, a.submitLink)

	a.router.Get(metricEndpoint, a.metricPrometheus())

	a.router.HandleFunc("/index/json", a.indexJSON())

	a.router.NotFound(a.render404Page)

	return &a, nil
}

// Run implements service.Service
func (a *API) Run(ctx context.Context) error {
	l, err := net.Listen("tcp", a.cfg.ListenAddr)
	if err != nil {
		return err
	}
	defer func() { _ = l.Close() }()

	srv := &http.Server{
		Addr:    a.cfg.ListenAddr,
		Handler: a.router,
	}

	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()

	// a.cfg.Logger.WithField("addr", svc.cfg.ListenAddr).Info("starting front-end server")
	if err = srv.Serve(l); err == http.ErrServerClosed {
		// Ignore error when the server shuts down.
		err = nil
	}

	return err
}

func (a *API) metricPrometheus() http.HandlerFunc {
	return promhttp.Handler().ServeHTTP
}

func (a *API) indexJSON() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		offset, err := strconv.Atoi(q)
		if err != nil {
			http.Error(w, "q offset is not number", 500)
			return
		}

		iter, err := a.cfg.IndexAPI.Search(index.Query{
			Type:       0,
			Expression: "",
			Offset:     uint64(offset),
		})
		if err != nil {
			log.Println("index/json :", err)
			http.Error(w, "index iter error ", 500)
			return
		}
		defer iter.Close()

		type indexScore struct {
			ID        string
			URL       string
			PageScore float64
		}
		docs := []*indexScore{}
		for iter.Next() {
			var idx indexScore
			doc := iter.Document()
			idx.ID = doc.LinkID.String()
			idx.URL = doc.URL
			idx.PageScore = doc.Pagerank
			docs = append(docs, &idx)
		}

		if err := iter.Error(); err != nil {
			log.Println("index/json :", err)
			http.Error(w, "index iter error ", 500)
			return
		}

		if err := json.NewEncoder(w).Encode(docs); err != nil {
			log.Println(err)
		}
	}
}

func (a *API) renderIndexPage(w http.ResponseWriter, r *http.Request) {
	_ = a.templateFunc(indexPageTemplate, w, map[string]interface{}{
		"searchEndpoint":     searchEndpoint,
		"submitLinkEndpoint": submitLinkEndpoint,
	})
}

func (a *API) render404Page(w http.ResponseWriter, _ *http.Request) {
	_ = a.templateFunc(msgPageTemplate, w, map[string]interface{}{
		"indexEndpoint":  indexEndpoint,
		"searchEndpoint": searchEndpoint,
		"messageTitle":   "Page not found",
		"messageContent": "Page not found.",
	})
}

func (a *API) renderSearchErrorPage(w http.ResponseWriter, searchTerms string) {
	w.WriteHeader(http.StatusInternalServerError)
	_ = a.templateFunc(msgPageTemplate, w, map[string]interface{}{
		"indexEndpoint":  indexEndpoint,
		"searchEndpoint": searchEndpoint,
		"searchTerms":    searchTerms,
		"messageTitle":   "Error",
		"messageContent": "An error occurred; please try again later.",
	})
}

func (a *API) submitLink(w http.ResponseWriter, r *http.Request) {
	var msg string
	defer func() {
		_ = a.templateFunc(submitLinkPageTemplate, w, map[string]interface{}{
			"indexEndpoint":      indexEndpoint,
			"submitLinkEndpoint": submitLinkEndpoint,
			"messageContent":     msg,
		})
	}()

	if r.Method == "POST" {
		if err := r.ParseForm(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			msg = "Invalid web site URL."
			return
		}
		link, err := url.Parse(r.Form.Get("link"))
		if err != nil || (link.Scheme != "http" && link.Scheme != "https") {
			w.WriteHeader(http.StatusBadRequest)
			msg = "Invalid web site URL."
			return
		}

		link.Fragment = ""
		if err = a.cfg.GraphAPI.UpsertLink(&linkgraph.Link{URL: link.String()}); err != nil {
			// a.cfg.Logger.WithField("err", err).Errorf("could not upsert link into link graph")
			w.WriteHeader(http.StatusInternalServerError)
			msg = "An error occurred while adding web site to our index; please try again later."
			return
		}

		msg = "Web site was successfully submitted!"
	} else {
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (a *API) renderSearchResults(w http.ResponseWriter, r *http.Request) {
	searchTerms := r.URL.Query().Get("q")
	offset, _ := strconv.ParseUint(r.URL.Query().Get("offset"), 10, 64)

	matchedDocs, pagination, err := a.runQuery(searchTerms, offset)
	if err != nil {
		// a.cfg.Logger.WithField("err", err).Errorf("search query execution failed")
		log.Println(err)
		a.renderSearchErrorPage(w, searchTerms)
		return
	}

	// Render results page
	if err := a.templateFunc(resultsPageTemplate, w, map[string]interface{}{
		"indexEndpoint":  indexEndpoint,
		"searchEndpoint": searchEndpoint,
		"searchTerms":    searchTerms,
		"pagination":     pagination,
		"results":        matchedDocs,
	}); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (a *API) runQuery(searchTerms string, offset uint64) ([]matchedDoc, *paginationDetails, error) {
	var query = index.Query{Type: index.QueryTypeMatch, Expression: searchTerms, Offset: offset}
	if strings.HasPrefix(searchTerms, `"`) && strings.HasSuffix(searchTerms, `"`) {
		query.Type = index.QueryTypePhrase
		searchTerms = strings.Trim(searchTerms, `"`)
	}

	resultIt, err := a.cfg.IndexAPI.Search(query)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = resultIt.Close() }()

	// Wrap each result in a matchedDoc shim and generate a short summary which
	// highlights the matching search terms.
	summarizer := newMatchSummarizer(searchTerms, a.cfg.MaxSummaryLength)
	highlighter := newMatchHighlighter(searchTerms)
	matchedDocs := make([]matchedDoc, 0, a.cfg.ResultsPerPage)
	for resCount := 0; resultIt.Next() && resCount < a.cfg.ResultsPerPage; resCount++ {
		doc := resultIt.Document()
		matchedDocs = append(matchedDocs, matchedDoc{
			doc: doc,
			summary: highlighter.Highlight(
				template.HTMLEscapeString(
					summarizer.MatchSummary(doc.Content),
				),
			),
		})
	}

	if err = resultIt.Error(); err != nil {
		return nil, nil, err
	}

	// Setup paginator and generate prev/next links
	pagination := &paginationDetails{
		From:  int(offset + 1),
		To:    int(offset) + len(matchedDocs),
		Total: int(resultIt.TotalCount()),
	}
	if offset > 0 {
		pagination.PrevLink = fmt.Sprintf("%s?q=%s", searchEndpoint, searchTerms)
		if prevOffset := int(offset) - a.cfg.ResultsPerPage; prevOffset > 0 {
			pagination.PrevLink += fmt.Sprintf("&offset=%d", prevOffset)
		}
	}
	if nextPageOffset := int(offset) + len(matchedDocs); nextPageOffset < pagination.Total {
		pagination.NextLink = fmt.Sprintf("%s?q=%s&offset=%d", searchEndpoint, searchTerms, nextPageOffset)
	}

	return matchedDocs, pagination, nil
}

// paginationDetails encapsulates the details for rendering a paginator component.
type paginationDetails struct {
	From     int
	To       int
	Total    int
	PrevLink string
	NextLink string
}

// mathcedDoc wraps an index.Document and provides convenience methods for
// rendering its contents in a search results view.
type matchedDoc struct {
	doc     *index.Document
	summary string
}

func (d *matchedDoc) HighlightedSummary() template.HTML { return template.HTML(d.summary) }
func (d *matchedDoc) URL() string                       { return d.doc.URL }
func (d *matchedDoc) Title() string {
	if d.doc.Title != "" {
		return d.doc.Title
	}
	return d.doc.URL
}
