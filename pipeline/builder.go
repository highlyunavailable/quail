package pipeline

import "errors"

type PipelineBuilder struct {
	source       Source
	transformers []Transformer
	sinks        []Sink
}

var ErrMultiplePipelinesUseSource = errors.New("Source cannot be referenced by multiple pipelines")

func (b *PipelineBuilder) Build() (*Pipeline, error) {
	pipeline := &Pipeline{}

	if b.source.References() > 0 {
		return nil, ErrMultiplePipelinesUseSource
	}
	b.source.Acquire()
	pipeline.source = b.source
	pipeline.transformers = b.transformers
	for _, s := range b.sinks {
		s.Acquire()
	}
	pipeline.sinks = b.sinks

	return pipeline, nil
}

func (b *PipelineBuilder) SetSource(s Source) *PipelineBuilder {
	b.source = s
	return b
}

func (b *PipelineBuilder) AddTransformer(t Transformer) *PipelineBuilder {
	b.transformers = append(b.transformers, t)
	return b
}

func (b *PipelineBuilder) AddSink(s Sink) *PipelineBuilder {
	b.sinks = append(b.sinks, s)
	return b
}
