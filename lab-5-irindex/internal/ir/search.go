package ir

// SearchBoolEval применяет алгебру Буля + NEAR/MSM + FIRST/LAST (границы документа) над индексом.
func SearchBoolEval(ix *InvIndex, query string) (MatchSet, Node, error) {
	n, err := Parse(query)
	if err != nil {
		return nil, nil, err
	}
	return Eval(ix, n), n, nil
}

// SearchBM25 возвращает упорядоченный список после булевого фильтра и BM25.
func SearchBM25(ix *InvIndex, query string, k1, b float64) ([]Scored, error) {
	ds, n, err := SearchBoolEval(ix, query)
	if err != nil {
		return nil, err
	}
	qterms := PositiveTerms(n)
	return BM25(ix, ds, qterms, k1, b), nil
}
