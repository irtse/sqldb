package sqldb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestCreateTable(t *testing.T) {
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

func TestAddColumn(t *testing.T) {

	db := Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer db.Close()

	old, err := db.Table("test").GetSchema()
	if err != nil {
		fmt.Println(err.Error())
	}
	db.Table("test").AddColumn("addcolumn", "integer")
	new, err := db.Table("test").GetSchema()
	if err != nil {
		fmt.Println(err.Error())
	}

	if len(old.Columns) == len(new.Columns) {
		t.Errorf("Column already exist")
	}
}

func TestInsert(t *testing.T) {

	db := Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer db.Close()

	vl := make(AssRow)
	vl["name"] = "toto"
	vl["description"] = "tata"

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
}

func TestUpdate(t *testing.T) {

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

func TestDelete(t *testing.T) {

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

func TestDeleteColumn(t *testing.T) {

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

func TestDeleteTable(t *testing.T) {
	db := Open("postgres", "host=127.0.0.1 port=5432 user=test password=test dbname=test sslmode=disable")
	defer db.Close()

	db.Table("test").DeleteTable()

	tbl, err := db.Table("test").GetSchema()
	if err != nil {
		fmt.Println(err.Error())
	}
	if len(tbl.Columns) != 0 {
		t.Errorf("Delete table failed")
	}
}
