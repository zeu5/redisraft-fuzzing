package main

type Mutator interface {
	Mutate(*Trace, *EventTrace) (*Trace, bool)
}

type randomMutator struct{}

func (r *randomMutator) Mutate(_ *Trace, _ *EventTrace) (*Trace, bool) {
	return nil, false
}

func RandomMutator() Mutator {
	return &randomMutator{}
}
