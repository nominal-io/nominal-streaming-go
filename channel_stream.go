package nominal_streaming

type ChannelStream[T Value] struct {
	batcher *batcher
	ref     channelReference
	enqueue func(NanosecondsUTC, T)
}

func (cs *ChannelStream[T]) Enqueue(timestamp NanosecondsUTC, value T) {
	cs.enqueue(timestamp, value)
}
