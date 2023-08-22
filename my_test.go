package sqldb

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

func TestMyCreateTable(t *testing.T) {
	db := Open("mysql", "test:test@tcp(127.0.0.1:3306)/test?parseTime=true")
	defer db.Close()

	byteValue, _ := os.ReadFile("test_table.json")

	var jsonSource TableInfo
	json.Unmarshal([]byte(byteValue), &jsonSource)

	err := db.CreateTable(jsonSource)
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

func TestMyAddColumn(t *testing.T) {

	db := Open("mysql", "test:test@tcp(127.0.0.1:3306)/test?parseTime=true")
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

func TestMyInsert(t *testing.T) {

	db := Open("mysql", "test:test@tcp(127.0.0.1:3306)/test?parseTime=true")
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

	var result map[string]interface{}
	byteValue, _ := os.ReadFile("insert.json")
	json.Unmarshal(byteValue, &result)
}

func TestMyUpdate(t *testing.T) {

	db := Open("mysql", "test:test@tcp(127.0.0.1:3306)/test?parseTime=true")
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

func TestMyDelete(t *testing.T) {

	db := Open("mysql", "test:test@tcp(127.0.0.1:3306)/test?parseTime=true")
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

func TestMyDeleteColumn(t *testing.T) {

	db := Open("mysql", "test:test@tcp(127.0.0.1:3306)/test?parseTime=true")
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

func TestMyDeleteTable(t *testing.T) {
	db := Open("mysql", "test:test@tcp(127.0.0.1:3306)/test?parseTime=true")
	defer db.Close()
	db.Table("test").DeleteTable()

}

func TestMyImportSchema(t *testing.T) {
	db := Open("mysql", "test:test@tcp(127.0.0.1:3306)/test?parseTime=true")
	defer db.Close()
	db.ImportSchema("pfn.json")
}

func TestMyClearImportSchema(t *testing.T) {
	db := Open("mysql", "test:test@tcp(127.0.0.1:3306)/test?parseTime=true")
	defer db.Close()
	db.ClearImportSchema("pfn.json")
}

func TestMyGetSchema(t *testing.T) {
	db := Open("mysql", "test:test@tcp(127.0.0.1:3306)/test?parseTime=true?charset=utf8mb4&collation=utf8mb4_unicode_ci")
	defer db.Close()
	data, err := db.GetSchema()
	if err != nil {
		fmt.Println(err.Error())
	}
	val, _ := json.Marshal(data)
	fmt.Println(string(val))
}

func TestMySaveSchema(t *testing.T) {
	db := Open("mysql", "test:test@tcp(127.0.0.1:3306)/test?parseTime=true")
	defer db.Close()
	err := db.SaveSchema("schema.json")
	if err != nil {
		fmt.Println(err.Error())
	}
}
func TestMyGenerateTemplate(t *testing.T) {
	db := Open("mysql", "test:test@tcp(127.0.0.1:3306)/test?parseTime=true")
	defer db.Close()
	err := db.GenerateSchemaTemplate("plantuml.tmpl", "schema.puml")
	if err != nil {
		fmt.Println(err.Error())
	}
}

func TestMyGenerateTableTemplate(t *testing.T) {
	db := Open("mysql", "test:test@tcp(127.0.0.1:3306)/test?parseTime=true")
	defer db.Close()
	err := db.GenerateTableTemplates("table.tmpl", "gen", "html")
	if err != nil {
		fmt.Println(err.Error())
	}
}
