package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/odit-bit/linkstore/linkgraph"
	"github.com/odit-bit/se/pagerank/bspgraph"
	"github.com/odit-bit/se/pagerank/calculator"
	"github.com/odit-bit/se/pagerank/partition"

	"go.uber.org/multierr"
)

// GraphAPI defines as set of API methods for fetching the links and edges from
// the link graph.
type GraphAPI interface {
	Links(fromID, toID uuid.UUID, retrievedBefore time.Time) (linkgraph.LinkIterator, error)
	Edges(fromID, toID uuid.UUID, updatedBefore time.Time) (linkgraph.EdgeIterator, error)
}

// IndexAPI defines a set of API methods for updating PageRank scores for
// indexed documents.
type IndexAPI interface {
	UpdateRank(linkID uuid.UUID, score float64) error
}

// Config encapsulates the settings for configuring the PageRank calculator
// service.
type Config struct {
	// An API for interating links and edges from the link graph.
	GraphAPI GraphAPI

	// An API for updating the PageRank score for indexed documents.
	IndexAPI IndexAPI

	// An API for detecting the partition assignments for this service.
	PartitionDetector partition.Detector

	// The number of workers to spin up for computing PageRank scores. If
	// not specified, a default value of 1 will be used instead.
	ComputeWorkers int

	// The time between subsequent crawler passes.
	UpdateInterval time.Duration

	// // The logger to use. If not defined an output-discarding logger will
	// // be used instead.
	// Logger *logrus.Entry
}

func (cfg *Config) validate() error {
	var err error
	if cfg.GraphAPI == nil {
		err = multierr.Append(err, fmt.Errorf("graph API has not been provided"))
	}
	if cfg.IndexAPI == nil {
		err = multierr.Append(err, fmt.Errorf("index API has not been provided"))
	}
	if cfg.PartitionDetector == nil {
		err = multierr.Append(err, fmt.Errorf("partition detector has not been provided"))
	}

	if cfg.ComputeWorkers <= 0 {
		err = multierr.Append(err, fmt.Errorf("invalid value for compute workers"))
	}
	if cfg.UpdateInterval == 0 {
		err = multierr.Append(err, fmt.Errorf("invalid value for update interval"))
	}
	// if cfg.Logger == nil {
	// 	cfg.Logger = logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard})
	// }
	return err
}

// Service implements the PageRank calculator component.
type Service struct {
	cfg        Config
	calculator *calculator.Calculator

	logger *log.Logger
}

// create instannce with default configuration
func New(graphDB GraphAPI, indexDB IndexAPI) *Service {
	partition := partition.Fixed{Partition: 0, NumPartitions: 1}

	Conf := Config{
		GraphAPI:          graphDB,
		IndexAPI:          indexDB,
		PartitionDetector: partition,
		ComputeWorkers:    1,
		UpdateInterval:    30 * time.Minute,
	}

	svc, err := NewWithConfig(Conf)
	if err != nil {
		log.Fatal(err)
	}
	return svc
}

// NewService creates a new PageRank calculator service instance with the specified config.
func NewWithConfig(cfg Config) (*Service, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("pagerank service: config validation failed: %w", err)
	}

	calculator, err := calculator.NewCalculator(calculator.Config{ComputeWorkers: cfg.ComputeWorkers})
	if err != nil {
		return nil, fmt.Errorf("pagerank service: config validation failed: %w", err)
	}

	logger := log.New(os.Stdout, "[pagerank]", log.Ldate|log.Ltime)
	return &Service{
		cfg:        cfg,
		calculator: calculator,
		logger:     logger,
	}, nil
}

// Name implements service.Service
func (svc *Service) Name() string { return "PageRank calculator" }

// Run implements service.Service
func (svc *Service) Run(ctx context.Context) error {

	svc.logger.Println("pagerank service start")
	svc.logger.Printf("update interval: %v\n", svc.cfg.UpdateInterval.String())
	svc.logger.Printf("worker: %v\n", svc.cfg.ComputeWorkers)

	timer := time.NewTimer(svc.cfg.UpdateInterval)
	defer func() {
		if timer.Stop() {
			<-timer.C
		}

	}()

	for {
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
			svc.logger.Println("[INFO] pagerank iteration start")
			curPartition, _, err := svc.cfg.PartitionDetector.PartitionInfo()
			if err != nil {
				if errors.Is(err, partition.ErrNoPartitionDataAvailableYet) {
					svc.logger.Println("[WARN] deferring PageRank update pass: partition data not yet available")
					continue
				}
				return err
			}

			if curPartition != 0 {
				svc.logger.Println("[INFO] service can only run on the leader of the application cluster")
				return nil
			}

			if err := svc.updateGraphScores(ctx); err != nil {
				return err
			}
			timer.Reset(svc.cfg.UpdateInterval)
		}
	}
}

func (svc *Service) updateGraphScores(ctx context.Context) error {
	svc.logger.Println("[INFO] starting PageRank update pass",
		"vertice_count", len(svc.calculator.Graph().Vertices()))

	startAt := time.Now()

	maxUUID := uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	// tick := startAt
	if err := svc.calculator.Graph().Reset(); err != nil {
		svc.logger.Println("reset", err)
		return err
	} else if err := svc.loadLinks(uuid.Nil, maxUUID, startAt); err != nil {
		svc.logger.Println("loadlink", err)
		return err
	} else if err := svc.loadEdges(uuid.Nil, maxUUID, startAt); err != nil {
		svc.logger.Println("loadedges", err)
		return err
	}
	// graphPopulateTime := time.Since(tick)

	// tick = time.Now()
	if err := svc.calculator.Executor().RunToCompletion(ctx); err != nil {
		svc.logger.Println("[ERROR] run to completion", err)
		return err
	}
	// scoreCalculationTime := time.Since(tick)

	// tick = time.Now()
	if err := svc.calculator.Scores(svc.persistScore); err != nil {
		svc.logger.Println("[ERROR] persist score", err)
		return err
	}
	// scorePersistTime := time.Since(tick)

	proccessedLink := len(svc.calculator.Graph().Vertices())
	// graph_populate_time := graphPopulateTime.String()
	// score_calculation_time := scoreCalculationTime.String()
	// score_persist_time := scorePersistTime.String()
	total_pass_time := time.Since(startAt).Round(1 * time.Second)

	// svc.logger.Printf("proccessedLink:[%v]\n\tgraph_populate_time: [%v]\n\t score_calculation_time: [%v]\n\t score_persist_time: [%v]\n\t total_pass_time: [%v]\n",
	// 	proccessedLink,
	// 	graph_populate_time,
	// 	score_calculation_time,
	// 	score_persist_time,
	// 	total_pass_time,
	// )
	svc.logger.Printf("[INFO] completed PageRank update pass vertices:%v time:%v", proccessedLink, total_pass_time)

	return nil
}

func (svc *Service) persistScore(vertexID string, score float64) error {
	linkID, err := uuid.Parse(vertexID)
	if err != nil {
		return err
	}

	return svc.cfg.IndexAPI.UpdateRank(linkID, score)
}

func (svc *Service) loadLinks(fromID, toID uuid.UUID, filter time.Time) error {
	linkIt, err := svc.cfg.GraphAPI.Links(fromID, toID, filter)
	if err != nil {
		return err
	}
	count := 0
	for linkIt.Next() {

		link := linkIt.Link()
		svc.calculator.AddVertex(link.ID.String())
		count++
	}
	if err = linkIt.Error(); err != nil {
		_ = linkIt.Close()
		return err
	}

	return linkIt.Close()
}

func (svc *Service) loadEdges(fromID, toID uuid.UUID, filter time.Time) error {
	edgeIt, err := svc.cfg.GraphAPI.Edges(fromID, toID, filter)
	if err != nil {
		return err
	}

	count := 0
	for edgeIt.Next() {
		edge := edgeIt.Edge()
		// As new edges may have been created since the links were loaded be
		// tolerant to UnknownEdgeSource errors.
		if err = svc.calculator.AddEdge(edge.Src.String(), edge.Dst.String()); err != nil && !errors.Is(err, bspgraph.ErrUnknownEdgeSource) {
			_ = edgeIt.Close()
			return err
		}
		count++
	}

	if err = edgeIt.Error(); err != nil {
		_ = edgeIt.Close()
		return err
	}
	return edgeIt.Close()
}
