package dataset

import (
	"path/filepath"
	"strings"
	"testing"
)

// Tiny hand-written fixtures in each source's format (not real data).
const arffFixture = `% comment
@relation creditcard
@attribute Time numeric
@attribute V1 numeric
@attribute Amount numeric
@attribute Class {'0','1'}
@data
0,-1.35,149.62,'0'
0,1.19,2.69,'0'
1,-1.35,0.25,'0'
2,0.5,378.66,'1'
`

const paysimFixture = `step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud
1,PAYMENT,9839.64,C1231006815,170136.0,160296.36,M1979787155,0.0,0.0,0,0
1,TRANSFER,181.0,C1305486145,181.0,0.0,C553264065,0.0,0.0,1,0
2,PAYMENT,0.10,C840083671,181.0,0.0,M38997010,21182.0,0.0,0,0
3,PAYMENT,11668.14,C2048537720,41554.0,29885.86,M1230701703,0.0,0.0,1,0
`

func TestParseULBARFF(t *testing.T) {
	var got []Txn
	read, dropped, err := ParseULBARFF(strings.NewReader(arffFixture), func(t Txn) { got = append(got, t) })
	if err != nil {
		t.Fatal(err)
	}
	if read != 4 || dropped["amount_below_minimum"] != 1 || len(got) != 3 {
		t.Fatalf("read=%d dropped=%v kept=%d", read, dropped, len(got))
	}
	if got[0].AmountMinor != 14962 || got[1].AmountMinor != 269 || got[2].Label != 1 || got[2].Row != 3 || got[2].Currency != "EUR" {
		t.Fatalf("unexpected rows %+v", got)
	}
}

func TestParsePaySim(t *testing.T) {
	var got []Txn
	read, dropped, err := ParsePaySimCSV(strings.NewReader(paysimFixture), "USD", 1, func(t Txn) { got = append(got, t) })
	if err != nil {
		t.Fatal(err)
	}
	if read != 4 || dropped["type_not_selected"] != 1 || dropped["amount_below_minimum"] != 1 || len(got) != 2 {
		t.Fatalf("read=%d dropped=%v kept=%d", read, dropped, len(got))
	}
	if got[0].MerchantKey != "M1979787155" || got[0].TimeS < 0 || got[0].TimeS >= 3600 || got[1].TimeS < 7200 || got[1].Label != 1 {
		t.Fatalf("unexpected rows %+v", got)
	}
}

func TestNormalizedRoundTrip(t *testing.T) {
	in := []Txn{{Row: 2, TimeS: 5, AmountMinor: 100, Currency: "EUR", Label: 1}, {Row: 1, TimeS: 1.5, AmountMinor: 75, Currency: "EUR", MerchantKey: "M1"}}
	path := filepath.Join(t.TempDir(), "n.csv")
	sum, err := WriteNormalized(path, in)
	if err != nil {
		t.Fatal(err)
	}
	out, sum2, err := ReadNormalized(path)
	if err != nil {
		t.Fatal(err)
	}
	if sum != sum2 || len(out) != 2 || out[0].Row != 1 || out[0].MerchantKey != "M1" || out[1].Label != 1 {
		t.Fatalf("round trip: %v %v %+v", sum, sum2, out)
	}
}

// Hand-written in the layout of the Kaggle file (note the two "Account"
// columns); not real data.
const amlFixture = `Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,Amount Paid,Payment Currency,Payment Format,Is Laundering
2022/09/01 00:20,010,8000EBD30,0012,8000EBD30,3697.34,US Dollar,3697.34,US Dollar,Reinvestment,0
2022/09/01 00:02,011,8000F4580,0020,8000F5340,0.01,US Dollar,0.01,US Dollar,Credit Card,0
2022/09/01 00:05,011,8000F4580,0020,8000F5340,1120.50,US Dollar,1120.50,US Dollar,Credit Card,0
2022/09/01 00:07,001,8000F5030,0027,8000F5340,500.00,Euro,500.00,Euro,Credit Card,0
2022/09/01 01:01,012,8000F5000,0020,8000F5340,70.00,US Dollar,70.00,US Dollar,Credit Card,1
2022/09/01 01:03,012,8000F5000,0021,8000F9999,15.25,US Dollar,15.25,US Dollar,Wire,0
`

func TestParseIBMAML(t *testing.T) {
	var got []Txn
	read, dropped, err := ParseIBMAML(strings.NewReader(amlFixture), 1, IBMAMLOptions{}, func(t Txn) { got = append(got, t) })
	if err != nil {
		t.Fatal(err)
	}
	if read != 6 || len(got) != 2 || dropped["format_not_selected"] != 2 || dropped["currency_not_selected"] != 1 || dropped["amount_below_minimum"] != 1 {
		t.Fatalf("read=%d dropped=%v kept=%+v", read, dropped, got)
	}
	// The receiving account is the second Account column, not the sender's.
	if got[0].MerchantKey != "0020:8000F5340" || got[0].AmountMinor != 112050 || got[0].Currency != "USD" || got[0].Label != 0 {
		t.Fatalf("first kept row %+v", got[0])
	}
	// Times are relative to the first kept row's minute (00:05) and spread
	// within the minute; the 01:01 row is 56 minutes later.
	if got[0].TimeS < 0 || got[0].TimeS >= 60 || got[1].TimeS < 56*60 || got[1].TimeS >= 57*60 {
		t.Fatalf("times %v %v", got[0].TimeS, got[1].TimeS)
	}
	if got[1].Label != 0 {
		t.Fatal("laundering label carried without -laundering-label")
	}

	got = nil
	_, _, err = ParseIBMAML(strings.NewReader(amlFixture), 1, IBMAMLOptions{Formats: []string{"Credit Card", "Wire"}, LaunderingLabel: true, Limit: 2}, func(t Txn) { got = append(got, t) })
	if err != nil || len(got) != 2 {
		t.Fatalf("limit/formats: %v %+v", err, got)
	}
	got = nil
	if _, _, err = ParseIBMAML(strings.NewReader(amlFixture), 1, IBMAMLOptions{CurrencyName: "Euro", Currency: "EUR"}, func(t Txn) { got = append(got, t) }); err != nil || len(got) != 1 || got[0].Currency != "EUR" {
		t.Fatalf("euro filter: %v %+v", err, got)
	}
	if _, _, err = ParseIBMAML(strings.NewReader("a,b\n1,2\n"), 1, IBMAMLOptions{}, func(Txn) {}); err == nil || !strings.Contains(err.Error(), "lacks column") {
		t.Fatalf("wrong header accepted: %v", err)
	}
}

func TestParsePaySimTypesAndLimit(t *testing.T) {
	var got []Txn
	_, dropped, err := ParsePaySim(strings.NewReader(paysimFixture), "USD", 1, PaySimOptions{Types: []string{"PAYMENT", "TRANSFER"}}, func(t Txn) { got = append(got, t) })
	if err != nil || len(got) != 3 || dropped["amount_below_minimum"] != 1 {
		t.Fatalf("types: %v kept=%d dropped=%v", err, len(got), dropped)
	}
	got = nil
	if _, _, err = ParsePaySim(strings.NewReader(paysimFixture), "USD", 1, PaySimOptions{Types: []string{"PAYMENT", "TRANSFER"}, Limit: 1}, func(t Txn) { got = append(got, t) }); err != nil || len(got) != 1 {
		t.Fatalf("limit: %v %d", err, len(got))
	}
}
