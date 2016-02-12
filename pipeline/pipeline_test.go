package pipeline

import "testing"

type testSource struct {
	refCount
	outChan chan map[string]interface{}
}

func newTestSource() *testSource {
	return &testSource{
		outChan: make(chan map[string]interface{}, 5),
	}
}

func (s *testSource) OutChan() <-chan map[string]interface{} {
	return s.outChan
}

func (s *testSource) Run() error {
	for i := 1; i <= 10; i++ {
		s.outChan <- map[string]interface{}{"num": i, "foo": "bar"}
	}
	s.Stop()
	return nil
}

func (s *testSource) Stop() {
	close(s.outChan)
}

type testTransformer struct {
}

func (t *testTransformer) Transform(data map[string]interface{}) map[string]interface{} {
	data["num"] = data["num"].(int) * 2
	return data
}

func newTestSink(t *testing.T) *testSink {
	return &testSink{
		inChan: make(chan map[string]interface{}),
		t:      t,
	}
}

type testSink struct {
	refCount
	inChan chan map[string]interface{}
	t      *testing.T
}

func (s *testSink) InChan() chan<- map[string]interface{} {
	return s.inChan
}

func (s *testSink) Run() error {
	for i := range s.inChan {
		s.t.Logf("Got %+v", i)
		if i["num"].(int)%2 != 0 {
			s.t.Errorf("Expected numbers divisible by 2, got %d", i["num"])
		}
	}
	return nil
}

func (s *testSink) Stop() {
	s.Release()
	if s.References() == 0 {
		close(s.inChan)
	}
}

func Test_Pipeline(t *testing.T) {
	pb := PipelineBuilder{}
	pipe, err := pb.SetSource(newTestSource()).AddTransformer(&testTransformer{}).AddSink(newTestSink(t)).Build()
	if err != nil {
		t.Fatal(err)
	}
	pipe.Run()
}
