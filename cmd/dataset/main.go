// Command dataset downloads and normalizes public transaction datasets.
//
//	dataset fetch-ulb   [-dir data]                         download + verify + normalize ULB creditcard (OpenML 1597)
//	dataset paysim      -input PS_20174392719_1491204439457_log.csv [-dir data] [-currency USD] [-types PAYMENT] [-limit N]
//	dataset aml         -input HI-Small_Trans.csv [-dir data] [-formats "Credit Card"] [-currency-name "US Dollar" -currency USD] [-limit N]
//	dataset stats       -file data/ulb-creditcard.csv
package main

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"paymentledger/internal/dataset"
)

const (
	ulbDescriptionURL = "https://www.openml.org/api/v1/json/data/1597"
	ulbDownloadURL    = "https://openml.org/data/v1/download/1673544/creditcard.arff"
	// Published by OpenML for file 1673544; also re-checked against the live
	// API description at download time.
	ulbPublishedMD5 = "178bcf9bb1f31a3dfe12d0e577884add"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "fetch-ulb":
		err = fetchULB(os.Args[2:])
	case "paysim":
		err = paysim(os.Args[2:])
	case "aml":
		err = aml(os.Args[2:])
	case "stats":
		err = stats(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: dataset fetch-ulb [-dir data] | paysim -input FILE [-dir data] | aml -input FILE [-dir data] | stats -file FILE")
	os.Exit(2)
}

func fetchULB(args []string) error {
	fs := flag.NewFlagSet("fetch-ulb", flag.ExitOnError)
	dir := fs.String("dir", "data", "output directory")
	fs.Parse(args) //nolint:errcheck

	rawDir := filepath.Join(*dir, "raw", "ulb-creditcard")
	if err := os.MkdirAll(rawDir, 0o755); err != nil {
		return err
	}
	rawPath := filepath.Join(rawDir, "creditcard.arff")

	expected := ulbPublishedMD5
	if live, err := openMLChecksum(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not read OpenML description (%v); using pinned MD5\n", err)
	} else if live != ulbPublishedMD5 {
		return fmt.Errorf("OpenML now publishes MD5 %s, pinned %s: the upstream file changed; review before use", live, ulbPublishedMD5)
	}

	md5sum, shasum, size, err := fileDigests(rawPath)
	if err != nil || md5sum != expected {
		fmt.Fprintf(os.Stderr, "downloading %s\n", ulbDownloadURL)
		if err := download(ulbDownloadURL, rawPath); err != nil {
			return err
		}
		if md5sum, shasum, size, err = fileDigests(rawPath); err != nil {
			return err
		}
	} else {
		fmt.Fprintf(os.Stderr, "using cached %s\n", rawPath)
	}
	if md5sum != expected {
		return fmt.Errorf("MD5 mismatch for %s: got %s, OpenML publishes %s", rawPath, md5sum, expected)
	}
	fmt.Fprintf(os.Stderr, "verified MD5 %s (%d bytes)\n", md5sum, size)

	f, err := os.Open(rawPath)
	if err != nil {
		return err
	}
	defer f.Close()
	var txns []dataset.Txn
	read, dropped, err := dataset.ParseULBARFF(f, func(t dataset.Txn) { txns = append(txns, t) })
	if err != nil {
		return err
	}
	normPath := filepath.Join(*dir, "ulb-creditcard.csv")
	normSHA, err := dataset.WriteNormalized(normPath, txns)
	if err != nil {
		return err
	}
	m := dataset.Manifest{
		Name:      "ulb-creditcard",
		Source:    "OpenML dataset 1597 (creditcard), file 1673544",
		SourceURL: ulbDownloadURL,
		Citation: "Andrea Dal Pozzolo, Olivier Caelen, Reid A. Johnson and Gianluca Bontempi. Calibrating Probability with " +
			"Undersampling for Unbalanced Classification. IEEE CIDM 2015.",
		License: "As published on OpenML (listed as 'Public'); the Kaggle mirror lists ODbL. Check upstream terms before redistribution.",
		RawFile: rawPath, RawBytes: size, RawSHA256: shasum, RawMD5: md5sum, PublishedMD5: expected,
		RowsRead: read, RowsKept: len(txns), RowsDropped: dropped,
		NormalizedFile: normPath, NormalizedSHA256: normSHA, CreatedAt: time.Now().UTC(),
		Notes: []string{
			"Amounts are EUR; converted to cents with math.Round(amount*100).",
			fmt.Sprintf("Rows with amount below %d cents are dropped (processor minimum charge).", dataset.MinAmountMinor),
			"The dataset has no merchant field; the benchmark assigns merchants synthetically (see bench scenario).",
			"label = the dataset's Class (1 = fraud).",
		},
	}
	return writeManifest(filepath.Join(*dir, "ulb-creditcard.manifest.json"), m)
}

func paysim(args []string) error {
	fs := flag.NewFlagSet("paysim", flag.ExitOnError)
	input := fs.String("input", "", "PaySim CSV downloaded from https://www.kaggle.com/datasets/ealaxi/paysim1")
	dir := fs.String("dir", "data", "output directory")
	currency := fs.String("currency", "USD", "currency label to assign (PaySim amounts are in an unnamed local currency)")
	seed := fs.Uint64("seed", 1, "seed for within-hour timestamp spreading")
	types := fs.String("types", "PAYMENT", "comma-separated PaySim types to keep (PAYMENT, TRANSFER, CASH_OUT, CASH_IN, DEBIT); only PAYMENT is a customer paying a merchant")
	limit := fs.Int("limit", 0, "stop after this many kept rows (0 = all)")
	fs.Parse(args) //nolint:errcheck
	if *input == "" {
		return errors.New("-input is required")
	}
	md5sum, shasum, size, err := fileDigests(*input)
	if err != nil {
		return err
	}
	f, err := os.Open(*input)
	if err != nil {
		return err
	}
	defer f.Close()
	var txns []dataset.Txn
	read, dropped, err := dataset.ParsePaySim(f, *currency, *seed, dataset.PaySimOptions{Types: splitList(*types), Limit: *limit}, func(t dataset.Txn) { txns = append(txns, t) })
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		return err
	}
	normPath := filepath.Join(*dir, "paysim.csv")
	normSHA, err := dataset.WriteNormalized(normPath, txns)
	if err != nil {
		return err
	}
	return writeManifest(filepath.Join(*dir, "paysim.manifest.json"), dataset.Manifest{
		Name: "paysim", Source: "PaySim synthetic mobile money dataset (Kaggle: ealaxi/paysim1)",
		SourceURL: "https://www.kaggle.com/datasets/ealaxi/paysim1",
		Citation:  "E. A. Lopez-Rojas, A. Elmir, S. Axelsson. PaySim: A financial mobile money simulator for fraud detection. EMSS 2016.",
		License:   "CC BY-SA 4.0 (per the Kaggle listing); verify upstream.",
		RawFile:   *input, RawBytes: size, RawSHA256: shasum, RawMD5: md5sum,
		RowsRead: read, RowsKept: len(txns), RowsDropped: dropped,
		NormalizedFile: normPath, NormalizedSHA256: normSHA, CreatedAt: time.Now().UTC(),
		Notes: []string{
			"Rows kept: type in " + *types + "; merchant_key = nameDest.",
			"PaySim time resolution is one hour (step); rows are spread deterministically within their hour.",
			"Currency is a label chosen at conversion time: " + *currency,
		},
	})
}

func aml(args []string) error {
	fs := flag.NewFlagSet("aml", flag.ExitOnError)
	input := fs.String("input", "", "IBM AML transactions CSV (e.g. HI-Small_Trans.csv) from https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml")
	dir := fs.String("dir", "data", "output directory")
	name := fs.String("name", "aml", "output name: <dir>/<name>.csv and <name>.manifest.json")
	formats := fs.String("formats", "Credit Card", "comma-separated Payment Format values to keep")
	curName := fs.String("currency-name", "US Dollar", "dataset currency name to keep (rows paid and received in it)")
	currency := fs.String("currency", "USD", "ISO code written to the normalized file")
	launder := fs.Bool("laundering-label", false, "carry Is Laundering into the label column (labeled rows are then paid with a declining payment method)")
	seed := fs.Uint64("seed", 1, "seed for within-minute timestamp spreading")
	limit := fs.Int("limit", 0, "stop after this many kept rows (0 = all)")
	fs.Parse(args) //nolint:errcheck
	if *input == "" {
		return errors.New("-input is required")
	}
	md5sum, shasum, size, err := fileDigests(*input)
	if err != nil {
		return err
	}
	f, err := os.Open(*input)
	if err != nil {
		return err
	}
	defer f.Close()
	var txns []dataset.Txn
	opt := dataset.IBMAMLOptions{Formats: splitList(*formats), CurrencyName: *curName, Currency: *currency, LaunderingLabel: *launder, Limit: *limit}
	read, dropped, err := dataset.ParseIBMAML(f, *seed, opt, func(t dataset.Txn) { txns = append(txns, t) })
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		return err
	}
	normPath := filepath.Join(*dir, *name+".csv")
	normSHA, err := dataset.WriteNormalized(normPath, txns)
	if err != nil {
		return err
	}
	return writeManifest(filepath.Join(*dir, *name+".manifest.json"), dataset.Manifest{
		Name: *name, Source: "IBM synthetic transactions for anti-money-laundering research (Kaggle: ealtman2019/ibm-transactions-for-anti-money-laundering-aml; IBM/AML-Data)",
		SourceURL: "https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml",
		Citation:  "E. Altman, J. Blanusa, L. von Niederhausern, B. Egressy, A. Anghel, K. Atasu. Realistic Synthetic Financial Transactions for Anti-Money Laundering Models. NeurIPS 2023 Datasets and Benchmarks.",
		License:   "CDLA-Sharing-1.0 (the data; the IBM/AML-Data repository code is Apache-2.0)",
		RawFile:   *input, RawBytes: size, RawSHA256: shasum, RawMD5: md5sum,
		RowsRead: read, RowsKept: len(txns), RowsDropped: dropped,
		NormalizedFile: normPath, NormalizedSHA256: normSHA, CreatedAt: time.Now().UTC(),
		Notes: []string{
			"Rows kept: Payment Format in [" + *formats + "], paid and received in " + *curName + " (written as " + *currency + "); amount = Amount Received.",
			"merchant_key = <To Bank>:<receiving account>; these are accounts of individuals and companies, not merchants of record.",
			"AML time resolution is one minute; rows are spread deterministically within their minute; times are seconds after the first kept row.",
			fmt.Sprintf("Is Laundering carried into the label column: %v.", *launder),
			"The file is synthetic (multi-agent simulation). Account-to-account transfers are not Stripe payment events and carry no accounting records.",
		},
	})
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func stats(args []string) error {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	file := fs.String("file", "data/ulb-creditcard.csv", "normalized dataset")
	fs.Parse(args) //nolint:errcheck
	txns, sum, err := dataset.ReadNormalized(*file)
	if err != nil {
		return err
	}
	out := map[string]any{"file": *file, "sha256": sum, "stats": dataset.Summarize(txns)}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

func openMLChecksum() (string, error) {
	c := &http.Client{Timeout: 20 * time.Second}
	resp, err := c.Get(ulbDescriptionURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var d struct {
		Desc struct {
			MD5 string `json:"md5_checksum"`
		} `json:"data_set_description"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return "", err
	}
	if d.Desc.MD5 == "" {
		return "", errors.New("no md5_checksum in description")
	}
	return d.Desc.MD5, nil
}

func download(url, dst string) error {
	c := &http.Client{Timeout: 30 * time.Minute}
	resp, err := c.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	tmp := dst + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

func fileDigests(path string) (md5hex, shahex string, size int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", 0, err
	}
	defer f.Close()
	m, s := md5.New(), sha256.New()
	size, err = io.Copy(io.MultiWriter(m, s), f)
	return hex.EncodeToString(m.Sum(nil)), hex.EncodeToString(s.Sum(nil)), size, err
}

func writeManifest(path string, m dataset.Manifest) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "wrote %s (%d rows kept of %d; dropped %v)\n%s\n", m.NormalizedFile, m.RowsKept, m.RowsRead, m.RowsDropped, path)
	return nil
}
