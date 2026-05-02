package ir

// MatchSet — множество ID документов, удовлетворяющих запросу.
type MatchSet map[uint32]struct{}

func allDocs(ix *InvIndex) MatchSet {
	out := make(MatchSet, ix.NumDocs())
	for i := 0; i < ix.NumDocs(); i++ {
		out[uint32(i)] = struct{}{}
	}
	return out
}

func postingsDocSet(ix *InvIndex, term string) MatchSet {
	ps := ix.Postings(term)
	out := make(MatchSet, len(ps))
	for _, p := range ps {
		out[p.DocID] = struct{}{}
	}
	return out
}

func intersect(a, b MatchSet) MatchSet {
	if len(a) > len(b) {
		a, b = b, a
	}
	out := make(MatchSet)
	for id := range a {
		if _, ok := b[id]; ok {
			out[id] = struct{}{}
		}
	}
	return out
}

func union(a, b MatchSet) MatchSet {
	out := make(MatchSet, len(a)+len(b))
	for id := range a {
		out[id] = struct{}{}
	}
	for id := range b {
		out[id] = struct{}{}
	}
	return out
}

func subtract(a, b MatchSet) MatchSet {
	out := make(MatchSet)
	for id := range a {
		if _, ok := b[id]; !ok {
			out[id] = struct{}{}
		}
	}
	return out
}

// Eval выполняет булеву модель документов над индексом.
func Eval(ix *InvIndex, n Node) MatchSet {
	switch t := n.(type) {
	case *Term:
		return postingsDocSet(ix, t.Lex)
	case *Not:
		return subtract(allDocs(ix), Eval(ix, t.Child))
	case *And:
		if len(t.Children) == 0 {
			return MatchSet{}
		}
		ds := Eval(ix, t.Children[0])
		for i := 1; i < len(t.Children); i++ {
			ds = intersect(ds, Eval(ix, t.Children[i]))
		}
		return ds
	case *Or:
		return union(Eval(ix, t.Left), Eval(ix, t.Right))
	case *Near:
		return evalNear(ix, t.K, t.A, t.B)
	case *MSM:
		return evalMSM(ix, t.W, t.Terms)
	case *EdgeStart:
		return edgeStart(ix, t.Lex)
	case *EdgeEnd:
		return edgeEnd(ix, t.Lex)
	default:
		return MatchSet{}
	}
}

func evalNear(ix *InvIndex, k int, a, b string) MatchSet {
	if k < 0 {
		k = 0
	}
	da := ix.Postings(a)
	db := ix.Postings(b)
	out := MatchSet{}

	i, j := 0, 0
	for i < len(da) && j < len(db) {
		switch {
		case da[i].DocID < db[j].DocID:
			i++
		case da[i].DocID > db[j].DocID:
			j++
		default:
			doc := da[i].DocID
			if positionsNear(da[i].Poss, db[j].Poss, k) {
				out[doc] = struct{}{}
			}
			i++
			j++
		}
	}
	return out
}

func positionsNear(pa, pb []uint32, k int) bool {
	for _, x := range pa {
		// два указателя по отсортированным позициям
		for _, y := range pb {
			diff := int(x) - int(y)
			if diff < 0 {
				diff = -diff
			}
			if diff <= k {
				return true
			}
		}
	}
	return false
}

func edgeStart(ix *InvIndex, term string) MatchSet {
	ps := ix.Postings(term)
	out := MatchSet{}
	for _, p := range ps {
		if len(p.Poss) > 0 && p.Poss[0] == 0 {
			out[p.DocID] = struct{}{}
		}
	}
	return out
}

func edgeEnd(ix *InvIndex, term string) MatchSet {
	out := MatchSet{}
	for _, p := range ix.Postings(term) {
		last := len(ix.Docs[p.DocID].Tokens) - 1
		if last < 0 {
			continue
		}
		lp := uint32(last)
		for _, pos := range p.Poss {
			if pos == lp {
				out[p.DocID] = struct{}{}
				break
			}
		}
	}
	return out
}

func evalMSM(ix *InvIndex, w int, terms []string) MatchSet {
	if len(terms) == 0 {
		return allDocs(ix)
	}
	ds := postingsDocSet(ix, terms[0])
	for i := 1; i < len(terms); i++ {
		ds = intersect(ds, postingsDocSet(ix, terms[i]))
		if len(ds) == 0 {
			return ds
		}
	}
	if w < 0 {
		w = 0
	}
	out := MatchSet{}
	for id := range ds {
		if msmInDoc(ix.Docs[id], terms, w) {
			out[id] = struct{}{}
		}
	}
	return out
}

func msmInDoc(d Doc, terms []string, w int) bool {
	if len(d.Tokens) == 0 || len(terms) == 0 {
		return false
	}
	need := make(map[string]int)
	for _, t := range terms {
		need[t]++
	}
	type ev struct {
		pos uint32
		t   string
	}
	evs := make([]ev, 0, len(d.Tokens))
	for p, tok := range d.Tokens {
		if _, ok := need[tok]; ok {
			evs = append(evs, ev{uint32(p), tok})
		}
	}
	if len(evs) == 0 {
		return false
	}
	have := make(map[string]int, len(need))
	l := 0
	add := func(t string) { have[t]++ }
	sub := func(t string) { have[t]-- }
	satisfied := func() bool {
		for t, n := range need {
			if have[t] < n {
				return false
			}
		}
		return true
	}
	for r := range evs {
		add(evs[r].t)
		for satisfied() {
			if int(evs[r].pos-evs[l].pos) <= w {
				return true
			}
			sub(evs[l].t)
			l++
			if l > r {
				break
			}
		}
	}
	return false
}
