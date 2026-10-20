// Package dataset converts public transaction datasets into one normalized
// CSV format that the benchmark replays:
//
//	row,time_s,amount_minor,currency,merchant_key,label
//
// row is the 0-based index of the source record, time_s the offset in
// seconds from the dataset's first transaction, amount_minor the amount in
// cents, merchant_key the source's merchant identifier ("" if the dataset
// has none), and label the source's fraud label (0/1).
//
// Supported sources:
//   - ULB "Credit Card Fraud Detection" (Dal Pozzolo et al., 2015): 284,807
//     card transactions by European cardholders over two days in Sept 2013,
//     distributed by OpenML as dataset 1597. Amounts are in EUR.
//   - PaySim (Lopez-Rojas et al., 2016): a synthetic mobile-money log
//     calibrated on real data. Only distributed via Kaggle (login required),
//     so it must be downloaded by the user; PAYMENT rows are used.
package dataset

import (
	"bufio"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"paymentledger/internal/faults"
)

type Txn struct {
	Row         int
	TimeS       float64
	AmountMinor int64
	Currency    string
	MerchantKey string
	Label       int
}

type Manifest struct {
	Name             string         `json:"name"`
	Source           string         `json:"source"`
	SourceURL        string         `json:"source_url"`
	Citation         string         `json:"citation"`
	License          string         `json:"license"`
	RawFile          string         `json:"raw_file"`
	RawBytes         int64          `json:"raw_bytes"`
	RawSHA256        string         `json:"raw_sha256"`
	RawMD5           string         `json:"raw_md5,omitempty"`
	PublishedMD5     string         `json:"published_md5,omitempty"`
	RowsRead         int            `json:"rows_read"`
	RowsKept         int            `json:"rows_kept"`
	RowsDropped      map[string]int `json:"rows_dropped"`
	NormalizedFile   string         `json:"normalized_file"`
	NormalizedSHA256 string         `json:"normalized_sha256"`
	Notes            []string       `json:"notes"`
	CreatedAt        time.Time      `json:"created_at"`
}

// MinAmountMinor: rows below the processor's minimum charge are dropped
// (Stripe requires at least 0.50 in most currencies).
const MinAmountMinor = 50

// ParseULBARFF streams the OpenML ARFF file of ULB creditcard.
func ParseULBARFF(r io.Reader, fn func(Txn)) (read int, dropped map[string]int, err error) {
	dropped = map[string]int{}
	br := bufio.NewReaderSize(r, 1<<20)
	var attrs []string
	inData := false
	row := 0
	iTime, iAmount, iClass := -1, -1, -1
	for {
		line, rerr := br.ReadString('\n')
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "%") {
			switch {
			case !inData && strings.HasPrefix(strings.ToLower(line), "@attribute"):
				f := strings.Fields(line)
				if len(f) < 2 {
					return read, dropped, fmt.Errorf("bad attribute line %q", line)
				}
				attrs = append(attrs, strings.Trim(f[1], `'"`))
			case !inData && strings.EqualFold(line, "@data"):
				inData = true
				for i, a := range attrs {
					switch a {
					case "Time":
						iTime = i
					case "Amount":
						iAmount = i
					case "Class":
						iClass = i
					}
				}
				if iTime < 0 || iAmount < 0 || iClass < 0 {
					return read, dropped, errors.New("ARFF header lacks Time/Amount/Class attributes")
				}
			case inData:
				fields := strings.Split(line, ",")
				if len(fields) != len(attrs) {
					return read, dropped, fmt.Errorf("data row %d has %d fields, want %d", row, len(fields), len(attrs))
				}
				read++
				t, err1 := strconv.ParseFloat(fields[iTime], 64)
				amt, err2 := strconv.ParseFloat(fields[iAmount], 64)
				class := strings.Trim(fields[iClass], `'"`)
				if err1 != nil || err2 != nil || (class != "0" && class != "1") {
					return read, dropped, fmt.Errorf("data row %d unparseable", row)
				}
				minor := int64(math.Round(amt * 100))
				if minor < MinAmountMinor {
					dropped["amount_below_minimum"]++
				} else {
					fn(Txn{Row: row, TimeS: t, AmountMinor: minor, Currency: "EUR", Label: int(class[0] - '0')})
				}
				row++
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return read, dropped, rerr
		}
	}
	if !inData {
		return read, dropped, errors.New("no @data section")
	}
	return read, dropped, nil
}

// PaySimOptions selects which PaySim rows become benchmark transactions.
type PaySimOptions struct {
	// Types lists the PaySim "type" values to keep (PAYMENT, TRANSFER,
	// CASH_OUT, CASH_IN, DEBIT). Empty means PAYMENT only. Only PAYMENT is a
	// customer paying a merchant; the others move money between customers or
	// to cash agents and are offered for contention experiments, not as card
	// payments.
	Types []string
	// Limit stops after this many kept rows (0 = no limit).
	Limit int
}

// ParsePaySimCSV reads the Kaggle PaySim CSV and keeps PAYMENT rows (a
// customer paying a merchant, i.e. the card-payment-like flow). PaySim's
// clock is hourly ("step"); the position within the hour is unknown, so
// rows are spread deterministically within their hour.
func ParsePaySimCSV(r io.Reader, currency string, seed uint64, fn func(Txn)) (read int, dropped map[string]int, err error) {
	return ParsePaySim(r, currency, seed, PaySimOptions{}, fn)
}

// ParsePaySim is ParsePaySimCSV with options.
func ParsePaySim(r io.Reader, currency string, seed uint64, opt PaySimOptions, fn func(Txn)) (read int, dropped map[string]int, err error) {
	dropped = map[string]int{}
	want := map[string]bool{}
	for _, t := range opt.Types {
		want[strings.TrimSpace(t)] = true
	}
	if len(want) == 0 {
		want["PAYMENT"] = true
	}
	cr := csv.NewReader(bufio.NewReaderSize(r, 1<<20))
	cr.ReuseRecord = true
	header, err := cr.Read()
	if err != nil {
		return 0, dropped, err
	}
	idx := map[string]int{}
	for i, h := range header {
		idx[strings.TrimSpace(h)] = i
	}
	for _, need := range []string{"step", "type", "amount", "nameDest", "isFraud"} {
		if _, ok := idx[need]; !ok {
			return 0, dropped, fmt.Errorf("PaySim CSV lacks column %q (found %v)", need, header)
		}
	}
	kept, row := 0, 0
	for opt.Limit <= 0 || kept < opt.Limit {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return read, dropped, err
		}
		read++
		thisRow := row
		row++
		if !want[rec[idx["type"]]] {
			dropped["type_not_selected"]++
			continue
		}
		step, err1 := strconv.Atoi(rec[idx["step"]])
		amt, err2 := strconv.ParseFloat(rec[idx["amount"]], 64)
		fraud, err3 := strconv.Atoi(rec[idx["isFraud"]])
		if err1 != nil || err2 != nil || err3 != nil {
			return read, dropped, fmt.Errorf("row %d unparseable", thisRow)
		}
		minor := int64(math.Round(amt * 100))
		if minor < MinAmountMinor {
			dropped["amount_below_minimum"]++
			continue
		}
		if minor > MaxAmountMinor {
			dropped["amount_above_maximum"]++
			continue
		}
		offset := faults.Uniform(seed, "paysim_offset", strconv.Itoa(thisRow)) * 3600
		fn(Txn{Row: thisRow, TimeS: float64(step-1)*3600 + offset, AmountMinor: minor, Currency: currency,
			MerchantKey: rec[idx["nameDest"]], Label: fraud})
		kept++
	}
	return read, dropped, nil
}

// MaxAmountMinor: rows above this are dropped (the API's own amount limit).
const MaxAmountMinor = 99_999_999

// IBMAMLOptions selects which rows of an IBM "Transactions for Anti Money
// Laundering" file (HI/LI-Small/Medium/Large_Trans.csv) become transactions.
type IBMAMLOptions struct {
	// Formats lists the "Payment Format" values to keep. Empty means
	// "Credit Card": a customer paying a company by card is the closest
	// match to a card payment to a merchant. Other formats (Wire, ACH,
	// Cheque, Cash, Reinvestment, Bitcoin) are account-to-account transfers.
	Formats []string
	// CurrencyName is the dataset's currency name to keep (e.g. "US Dollar");
	// only rows paid and received in it are kept, so no FX is involved.
	// Currency is the ISO code written to the normalized file (e.g. "USD").
	CurrencyName, Currency string
	// LaunderingLabel carries "Is Laundering" into the label column. A
	// labeled row is then paid with the scenario's fraud payment method
	// (a decline by default). Laundering is not card fraud, so by default
	// every label is 0.
	LaunderingLabel bool
	Limit           int
}

// ParseIBMAML streams an IBM AML transactions CSV. Columns used:
// Timestamp ("2006/01/02 15:04"), To Bank, the receiving Account (the
// "Account" column after "To Bank"), Amount Received, Receiving Currency,
// Payment Currency, Payment Format, Is Laundering. Time resolution is one
// minute, so rows are spread deterministically within their minute; times are
// seconds after the first kept row's minute. merchant_key is "<To Bank>:<receiving account>".
func ParseIBMAML(r io.Reader, seed uint64, opt IBMAMLOptions, fn func(Txn)) (read int, dropped map[string]int, err error) {
	dropped = map[string]int{}
	want := map[string]bool{}
	for _, f := range opt.Formats {
		want[strings.TrimSpace(f)] = true
	}
	if len(want) == 0 {
		want["Credit Card"] = true
	}
	if opt.CurrencyName == "" {
		opt.CurrencyName = "US Dollar"
	}
	if opt.Currency == "" {
		opt.Currency = "USD"
	}
	cr := csv.NewReader(bufio.NewReaderSize(r, 1<<20))
	cr.ReuseRecord = true
	header, err := cr.Read()
	if err != nil {
		return 0, dropped, err
	}
	col := map[string]int{}
	toBank, recvAcct := -1, -1
	for i, h := range header {
		h = strings.TrimSpace(h)
		if _, dup := col[h]; !dup {
			col[h] = i
		}
		if h == "To Bank" {
			toBank = i
		}
		if toBank >= 0 && recvAcct < 0 && i > toBank && (h == "Account" || strings.HasPrefix(h, "Account.")) {
			recvAcct = i
		}
	}
	for _, need := range []string{"Timestamp", "To Bank", "Amount Received", "Receiving Currency", "Payment Currency", "Payment Format", "Is Laundering"} {
		if _, ok := col[need]; !ok {
			return 0, dropped, fmt.Errorf("IBM AML CSV lacks column %q (found %v)", need, header)
		}
	}
	if recvAcct < 0 {
		return 0, dropped, fmt.Errorf("IBM AML CSV has no receiving Account column after To Bank (found %v)", header)
	}
	var base time.Time
	kept, row := 0, 0
	for opt.Limit <= 0 || kept < opt.Limit {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return read, dropped, err
		}
		read++
		thisRow := row
		row++
		if !want[rec[col["Payment Format"]]] {
			dropped["format_not_selected"]++
			continue
		}
		if rec[col["Receiving Currency"]] != opt.CurrencyName || rec[col["Payment Currency"]] != opt.CurrencyName {
			dropped["currency_not_selected"]++
			continue
		}
		ts, err1 := time.Parse("2006/01/02 15:04", rec[col["Timestamp"]])
		amt, err2 := strconv.ParseFloat(rec[col["Amount Received"]], 64)
		laundering, err3 := strconv.Atoi(rec[col["Is Laundering"]])
		if err1 != nil || err2 != nil || err3 != nil {
			return read, dropped, fmt.Errorf("row %d unparseable", thisRow)
		}
		minor := int64(math.Round(amt * 100))
		if minor < MinAmountMinor {
			dropped["amount_below_minimum"]++
			continue
		}
		if minor > MaxAmountMinor {
			dropped["amount_above_maximum"]++
			continue
		}
		if base.IsZero() {
			base = ts
		}
		label := 0
		if opt.LaunderingLabel {
			label = laundering
		}
		fn(Txn{Row: thisRow, TimeS: ts.Sub(base).Seconds() + faults.Uniform(seed, "aml_offset", strconv.Itoa(thisRow))*60,
			AmountMinor: minor, Currency: opt.Currency, MerchantKey: rec[toBank] + ":" + rec[recvAcct], Label: label})
		kept++
	}
	return read, dropped, nil
}

// WriteNormalized writes txns (sorted by time, then row) and returns the
// file's SHA-256.
func WriteNormalized(path string, txns []Txn) (string, error) {
	sort.SliceStable(txns, func(i, j int) bool {
		if txns[i].TimeS != txns[j].TimeS {
			return txns[i].TimeS < txns[j].TimeS
		}
		return txns[i].Row < txns[j].Row
	})
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	w := bufio.NewWriter(io.MultiWriter(f, h))
	fmt.Fprintln(w, "row,time_s,amount_minor,currency,merchant_key,label")
	for _, t := range txns {
		fmt.Fprintf(w, "%d,%s,%d,%s,%s,%d\n", t.Row, strconv.FormatFloat(t.TimeS, 'f', -1, 64), t.AmountMinor, t.Currency, t.MerchantKey, t.Label)
	}
	if err := w.Flush(); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), f.Close()
}

// ReadNormalized loads a normalized CSV.
func ReadNormalized(path string) ([]Txn, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(raw)
	cr := csv.NewReader(strings.NewReader(string(raw)))
	cr.ReuseRecord = true
	header, err := cr.Read()
	if err != nil {
		return nil, "", err
	}
	if strings.Join(header, ",") != "row,time_s,amount_minor,currency,merchant_key,label" {
		return nil, "", fmt.Errorf("%s: not a normalized dataset (header %v)", path, header)
	}
	var out []Txn
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, "", err
		}
		var t Txn
		var e1, e2, e3, e4 error
		t.Row, e1 = strconv.Atoi(rec[0])
		t.TimeS, e2 = strconv.ParseFloat(rec[1], 64)
		t.AmountMinor, e3 = strconv.ParseInt(rec[2], 10, 64)
		t.Currency, t.MerchantKey = rec[3], rec[4]
		t.Label, e4 = strconv.Atoi(rec[5])
		if err := errors.Join(e1, e2, e3, e4); err != nil {
			return nil, "", fmt.Errorf("%s line %d: %w", path, len(out)+2, err)
		}
		out = append(out, t)
	}
	return out, hex.EncodeToString(sum[:]), nil
}

type Stats struct {
	Rows          int     `json:"rows"`
	SpanSeconds   float64 `json:"span_seconds"`
	MeanRatePerS  float64 `json:"mean_rate_per_s"`
	PeakRate1s    int     `json:"peak_rate_1s"`
	AmountP50     int64   `json:"amount_p50_minor"`
	AmountP90     int64   `json:"amount_p90_minor"`
	AmountP99     int64   `json:"amount_p99_minor"`
	AmountMax     int64   `json:"amount_max_minor"`
	AmountTotal   int64   `json:"amount_total_minor"`
	Labeled       int     `json:"label_1_rows"`
	DistinctMerch int     `json:"distinct_merchant_keys"`
}

func Summarize(txns []Txn) Stats {
	s := Stats{Rows: len(txns)}
	if len(txns) == 0 {
		return s
	}
	amts := make([]int64, len(txns))
	perSec := map[int64]int{}
	merch := map[string]bool{}
	minT, maxT := txns[0].TimeS, txns[0].TimeS
	for i, t := range txns {
		amts[i] = t.AmountMinor
		s.AmountTotal += t.AmountMinor
		s.Labeled += t.Label
		perSec[int64(t.TimeS)]++
		if t.MerchantKey != "" {
			merch[t.MerchantKey] = true
		}
		minT, maxT = math.Min(minT, t.TimeS), math.Max(maxT, t.TimeS)
	}
	sort.Slice(amts, func(i, j int) bool { return amts[i] < amts[j] })
	q := func(p float64) int64 { return amts[int(math.Min(float64(len(amts)-1), p*float64(len(amts))))] }
	s.AmountP50, s.AmountP90, s.AmountP99, s.AmountMax = q(0.5), q(0.9), q(0.99), amts[len(amts)-1]
	s.SpanSeconds = maxT - minT
	if s.SpanSeconds > 0 {
		s.MeanRatePerS = float64(len(txns)) / s.SpanSeconds
	}
	for _, n := range perSec {
		s.PeakRate1s = max(s.PeakRate1s, n)
	}
	s.DistinctMerch = len(merch)
	return s
}
