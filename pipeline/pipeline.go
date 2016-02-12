package pipeline

import (
	"sync"
	"sync/atomic"
)

// Defines a type that has a synchronous Run method that blocks until
// Stop is called from another goroutine or the Run method finishes.
type Runnable interface {
	Run() error
	Stop()
}

// Defines a type that must be initalized before use. Cleanup de-initalizes the
// type. The bools that each return signify whether the initialization or
// cleanup was actually performed - e.g. initing twice returns false on the
// second init call.
type Initable interface {
	Init() bool
	Cleanup() bool
}

type Source interface {
	Runnable
	ReferenceCounter
	OutChan() <-chan map[string]interface{}
}

type Transformer interface {
	Transform(map[string]interface{}) map[string]interface{}
}

type Sink interface {
	Runnable
	ReferenceCounter
	InChan() chan<- map[string]interface{}
}

type Pipeline struct {
	wg           sync.WaitGroup
	source       Source
	transformers TransformerSet
	sinks        []Sink
}

func (p *Pipeline) Run() error {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.source.Run()
	}()
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.transformers.Run(p.source, p.sinks)
	}()
	for _, s := range p.sinks {
		p.wg.Add(1)
		go func(s Sink) {
			defer p.wg.Done()
			s.Run()
		}(s)
	}
	p.wg.Wait()
	return nil
}

func (p *Pipeline) Stop() {}

type TransformerSet []Transformer

func (ts *TransformerSet) Run(src Source, sinks []Sink) {
	for data := range src.OutChan() {
		for _, t := range *ts {
			txdata := t.Transform(data)
			for _, sink := range sinks {
				sink.InChan() <- txdata
			}
		}
	}
	for _, sink := range sinks {
		sink.Stop()
	}
}

type ReferenceCounter interface {
	Acquire()
	Release()
	References() int32
}

type refCount struct {
	count int32
}

func (rc *refCount) Acquire() {
	atomic.AddInt32(&rc.count, 1)
}

func (rc *refCount) Release() {
	if atomic.AddInt32(&rc.count, -1) < 0 {
		panic("Reference count dropped below 0")
	}
}

func (rc *refCount) References() int32 {
	return atomic.LoadInt32(&rc.count)
}
