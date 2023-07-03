package sqldb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestPgCreateTable(t *testing.T) {
	db := Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer db.Close()

	jsonFile, err := os.Open("test_table.json")
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var jsonSource TableInfo
	json.Unmarshal([]byte(byteValue), &jsonSource)

	err = db.CreateTable(jsonSource)
	if err != nil {
		fmt.Println(err.Error())
	}

	sch, err := db.Table("test").GetSchema()
	if err != nil {
		fmt.Println(err.Error())
	}
	if len(sch.Columns) == 0 {
		t.Errorf("Create table failed")
	}
}

func TestPgAddColumn(t *testing.T) {

	db := Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer db.Close()

	old, err := db.Table("test").GetSchema()
	if err != nil {
		fmt.Println(err.Error())
	}
	db.Table("test").AddColumn("addcolumn", "integer", "comment")
	new, err := db.Table("test").GetSchema()
	if err != nil {
		fmt.Println(err.Error())
	}

	if len(old.Columns) == len(new.Columns) {
		t.Errorf("Column already exist")
	}
}

func TestPgInsert(t *testing.T) {

	db := Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer db.Close()

	vl := make(AssRow)
	vl["name"] = "toto"
	vl["description"] = "tata"
	vl["longitude"] = 1.38
	vl["enddate"] = "2022-09-01"
	vl["boolvalue"] = "true"

	old, err := db.Table("test").GetAssociativeArray([]string{"*"}, "", []string{}, "")
	if err != nil {
		fmt.Println(err.Error())
	}
	jsonStringOld, _ := json.Marshal(old)
	fmt.Println(string(jsonStringOld))

	db.Table("test").UpdateOrInsert(vl)

	new, err := db.Table("test").GetAssociativeArray([]string{"*"}, "", []string{}, "")
	if err != nil {
		fmt.Println(err.Error())
	}
	jsonStringNew, _ := json.Marshal(new)
	fmt.Println(string(jsonStringNew))

	if len(jsonStringOld) == len(jsonStringNew) {
		t.Errorf("Error row not created")
	}
	jsonFile, err := os.Open("insert.json")
	defer jsonFile.Close()
	var result map[string]interface{}
	byteValue, _ := ioutil.ReadAll(jsonFile)
	json.Unmarshal(byteValue, &result)
}

func TestPgUpdate(t *testing.T) {

	db := Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer db.Close()

	vl := make(AssRow)
	vl["id"] = 1
	vl["name"] = "titi"
	vl["description"] = "toto"

	old, err := db.Table("test").GetAssociativeArray([]string{"*"}, "", []string{}, "")
	if err != nil {
		fmt.Println(err.Error())
	}
	jsonStringOld, _ := json.Marshal(old)
	fmt.Println(string(jsonStringOld))

	db.Table("test").UpdateOrInsert(vl)

	new, err := db.Table("test").GetAssociativeArray([]string{"*"}, "", []string{}, "")
	if err != nil {
		fmt.Println(err.Error())
	}
	jsonStringNew, _ := json.Marshal(new)
	fmt.Println(string(jsonStringNew))

	if string(jsonStringOld) == string(jsonStringNew) {
		t.Errorf("Error row not updated")
	}

}

func TestPgDelete(t *testing.T) {

	db := Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer db.Close()

	vl := make(AssRow)
	vl["id"] = 1

	old, err := db.Table("test").GetAssociativeArray([]string{"*"}, "", []string{}, "")
	if err != nil {
		fmt.Println(err.Error())
	}
	jsonStringOld, _ := json.Marshal(old)
	fmt.Println(string(jsonStringOld))

	db.Table("test").Delete(vl)

	new, err := db.Table("test").GetAssociativeArray([]string{"*"}, "", []string{}, "")
	if err != nil {
		fmt.Println(err.Error())
	}
	jsonStringNew, _ := json.Marshal(new)
	fmt.Println(string(jsonStringNew))

	if len(jsonStringOld) == len(jsonStringNew) {
		t.Errorf("Error row not deleted")
	}
}

func TestPgDeleteColumn(t *testing.T) {

	db := Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer db.Close()

	old, err := db.Table("test").GetSchema()
	if err != nil {
		fmt.Println(err.Error())
	}
	db.Table("test").DeleteColumn("addcolumn")
	new, err := db.Table("test").GetSchema()
	if err != nil {
		fmt.Println(err.Error())
	}

	if len(old.Columns) == len(new.Columns) {
		t.Errorf("Error column not deleted")
	}
}

func TestPgDeleteTable(t *testing.T) {
	db := Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer db.Close()
	db.Table("test").DeleteTable()

}

func TestPgImportSchema(t *testing.T) {
	db := Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer db.Close()
	db.ImportSchema("pfn.json")
}

func TestPgClearImportSchema(t *testing.T) {
	db := Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer db.Close()
	db.ClearImportSchema("pfn.json")
}

func TestPgGetSchema(t *testing.T) {
	db := Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer db.Close()
	data, err := db.GetSchema()
	if err != nil {
		fmt.Println(err.Error())
	}
	val, _ := json.Marshal(data)
	fmt.Println(string(val))
}

func TestPgSaveSchema(t *testing.T) {
	db := Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer db.Close()
	err := db.SaveSchema("schema.json")
	if err != nil {
		fmt.Println(err.Error())
	}
}
func TestPgGenerateTemplate(t *testing.T) {
	db := Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer db.Close()
	err := db.GenerateTemplate("plantuml.tmpl", "schema.puml")
	if err != nil {
		fmt.Println(err.Error())
	}
}

func TestPgGenerateTableTemplate(t *testing.T) {
	db := Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer db.Close()
	err := db.GenerateTableTemplates("table.tmpl", "gen", "html")
	if err != nil {
		fmt.Println(err.Error())
	}
}
