package sqldb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

type Db struct {
	Driver string
	Url    string
	conn   *sql.DB
}

// AssRow : associative row type
type AssRow map[string]interface{}

// Select Result
type Rows []AssRow

// Table is a table structure description
type TableInfo struct {
	Name    string            `json:"name"`
	Columns map[string]string `json:"columns"`
	db      *Db
}

type Link struct {
	Source      string
	Destination string
}

// Open the database
func Open(driver string, url string) *Db {
	var database Db
	var err error
	database.Driver = driver
	database.Url = url
	database.conn, err = sql.Open(driver, url)
	if err != nil {
		log.Println(err)
	}
	return &database
}

// Close the database connection
func (db *Db) Close() {
	db.conn.Close()
}

func (db *Db) Table(name string) *TableInfo {
	var ti TableInfo
	ti.Name = name
	ti.db = db
	return &ti
}

// GetAssociativeArray : Provide results as an associative array
func (t *TableInfo) GetAssociativeArray(columns []string, restriction string, sortkeys []string, dir string) ([]AssRow, error) {
	return t.db.QueryAssociativeArray(t.buildSelect("", columns, restriction, sortkeys, dir))
}

// QueryAssociativeArray : Provide results as an associative array
func (db *Db) QueryAssociativeArray(query string) (Rows, error) {
	rows, err := db.conn.Query(query)
	if err != nil {
		log.Println(err)
		log.Println(query)
		return nil, err
	}
	defer rows.Close()

	var results Rows
	cols, err := rows.Columns()
	if err != nil {
		log.Println(err)
		log.Println(query)
		return nil, err
	}
	for rows.Next() {
		// Create a slice of interface{}'s to represent each column,
		// and a second slice to contain pointers to each item in the columns slice.
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {

		}
		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make(AssRow)
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			m[colName] = fmt.Sprintf("%v", *val)
		}

		results = append(results, m)

	}
	return results, nil
}

// GetSchema : Provide results as an associative array
func (t *TableInfo) GetSchema() (*TableInfo, error) {
	var ti TableInfo
	ti.Name = t.Name
	ti.db = t.db
	cols, err := t.db.QueryAssociativeArray("SELECT column_name :: varchar as name, REPLACE(REPLACE(data_type,'character varying','varchar'),'character','char') || COALESCE('(' || character_maximum_length || ')', '') as type, col_description('public." + t.Name + "'::regclass, ordinal_position) as comment  from INFORMATION_SCHEMA.COLUMNS where table_name ='" + t.Name + "';")
	if err != nil {
		log.Println(err)
		return nil, err
	}
	ti.Columns = make(map[string]string)
	for _, row := range cols {
		var name, rowtype, comment string
		for key, element := range row {
			if key == "name" {
				name = fmt.Sprintf("%v", element)
			}
			if key == "type" {
				rowtype = fmt.Sprintf("%v", element)
			}
			if key == "comment" {
				comment = fmt.Sprintf("%v", element)
			}
		}
		ti.Columns[name] = rowtype
		if comment != "<nil>" && strings.TrimSpace(comment) != "" {
			ti.Columns[name] = ti.Columns[name] + "|" + comment
		}
	}
	return &ti, nil
}

func (db *Db) GetSchema() ([]TableInfo, error) {
	var res []TableInfo
	tables, err := db.ListTables()
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}
	for _, row := range tables {
		for _, element := range row {
			var ti TableInfo
			var fullti *TableInfo
			ti.Name = fmt.Sprintf("%v", element)
			ti.db = db
			fullti, err = ti.GetSchema()
			if err != nil {
				log.Println(err.Error())
				return nil, err
			}
			res = append(res, *fullti)
		}
	}
	return res, nil
}

func (db *Db) ListTables() (Rows, error) {
	return db.QueryAssociativeArray("SELECT table_name :: varchar as name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;")
}

func (db *Db) CreateTable(t TableInfo) error {
	t.db = db
	query := "create table " + t.Name + " ( "
	columns := ""
	for name, rowtype := range t.Columns {
		if fmt.Sprintf("%v", name) == "id" {
			columns += fmt.Sprintf("%v", name) + " " + "SERIAL PRIMARY KEY,"
		} else {
			desc := strings.Split(fmt.Sprintf("%v", rowtype), "|")
			columns += fmt.Sprintf("%v", name) + " " + desc[0]
			columns += ","
		}
	}
	query += columns
	query = query[:len(query)-1] + " )"
	_, err := t.db.conn.Query(query)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	for name, rowtype := range t.Columns {
		desc := strings.Split(fmt.Sprintf("%v", rowtype), "|")
		if len(desc) > 1 {
			query = "COMMENT ON COLUMN " + t.Name + "." + fmt.Sprintf("%v", name) + " IS '" + desc[1] + "'"
			_, err := t.db.conn.Query(query)
			if err != nil {
				log.Println(err.Error())
				return err
			}
		}

	}
	return nil
}

func (t *TableInfo) DeleteTable() error {
	query := "drop table " + t.Name
	_, err := t.db.conn.Query(query)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	query = "drop sequence if exists sq_" + t.Name
	_, err = t.db.conn.Query(query)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	return nil
}

func (t *TableInfo) AddColumn(name string, sqltype string, comment string) error {
	query := "alter table " + t.Name + " add " + name + " " + sqltype
	rows, err := t.db.conn.Query(query)
	if err != nil {
		log.Println(err)
		return err
	}
	if strings.TrimSpace(comment) != "" {
		query = "COMMENT ON COLUMN " + t.Name + "." + name + " IS '" + comment + "'"
		_, err = t.db.conn.Query(query)
		if err != nil {
			log.Println(err.Error())
			return err
		}
	}
	defer rows.Close()
	return nil
}

func (t *TableInfo) DeleteColumn(name string) error {
	query := "alter table " + t.Name + " drop " + name
	rows, err := t.db.conn.Query(query)
	if err != nil {
		log.Println(err)
		return err
	}
	defer rows.Close()
	return nil
}

func (db *Db) ImportSchema(filename string) {
	jsonFile, err := os.Open(filename)
	if err != nil {
		log.Println(err)
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var jsonSource []TableInfo
	json.Unmarshal([]byte(byteValue), &jsonSource)
	for _, ti := range jsonSource {
		ti.db = db
		err = db.CreateTable(ti)
		if err != nil {
			log.Println(err.Error())
		}
	}
}

func (db *Db) ClearImportSchema(filename string) {
	jsonFile, err := os.Open(filename)
	if err != nil {
		log.Println(err)
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var jsonSource []TableInfo
	json.Unmarshal([]byte(byteValue), &jsonSource)
	for _, ti := range jsonSource {
		ti.db = db
		err = ti.DeleteTable()
		if err != nil {
			log.Println(err.Error())
		}
	}
}

func (db *Db) ListSequences() (Rows, error) {
	return db.QueryAssociativeArray("SELECT sequence_name :: varchar FROM information_schema.sequences WHERE sequence_schema = 'public' ORDER BY sequence_name;")
}

func (t *TableInfo) buildSelect(key string, columns []string, restriction string, sortkeys []string, dir ...string) string {
	if key != "" {
		columns = append(columns, key)
	}
	query := "select " + strings.Join(columns, ",") + " from " + t.Name
	if restriction != "" {
		query += " where " + restriction
	}
	if len(sortkeys) > 0 && len(sortkeys[0]) > 0 {
		query += " order by " + strings.Join(sortkeys, ",")
	}
	if len(dir) > 0 {
		query += " " + dir[0]
	}
	return query
}

func (t *TableInfo) Insert(record AssRow) (int, error) {
	columns := ""
	values := ""
	t, err := t.GetSchema()
	if err != nil {
		log.Println(err)
		return -1, err
	}
	var id int

	for key, element := range record {

		if strings.Contains(t.Columns[key], "char") || strings.Contains(t.Columns[key], "date") {
			columns += key + ","
			values += fmt.Sprint(pq.QuoteLiteral(fmt.Sprintf("%v", element))) + ","
		} else {

			columns += key + ","
			values += fmt.Sprintf("%v", element) + ","
		}
	}

	t.db.conn.QueryRow("INSERT INTO " + t.Name + "(" + removeLastChar(columns) + ") VALUES (" + removeLastChar(values) + ") RETURNING id").Scan(&id)
	return id, nil
}

func (t *TableInfo) Update(record AssRow) error {

	t, err := t.GetSchema()
	if err != nil {
		log.Println(err)
		return err
	}
	id := ""
	stack := ""

	for key, element := range record {

		if strings.Contains(t.Columns[key], "char") || strings.Contains(t.Columns[key], "date") {

			stack = stack + " " + key + " = " + pq.QuoteLiteral(fmt.Sprintf("%v", element)) + ","

		} else {

			if key == "id" {
				id = fmt.Sprintf("%v", element)
			} else {
				stack = stack + " " + key + " = " + fmt.Sprintf("%v", element) + ","
			}
		}
	}
	stack = removeLastChar(stack)
	query := ("UPDATE " + t.Name + " SET " + stack + " WHERE id = " + id)
	rows, err := t.db.conn.Query(query)
	if err != nil {
		log.Println(query)
		log.Println(err)
		return err
	}
	defer rows.Close()
	return nil
}

func (t *TableInfo) Delete(record AssRow) error {
	id := ""
	values := ""

	for key, element := range record {
		if key == "id" {
			values += fmt.Sprintf("%v", element) + ","
			id = removeLastChar(values)

		}
	}
	query := ("DELETE FROM " + t.Name + " WHERE id = " + id)
	rows, err := t.db.conn.Query(query)
	if err != nil {
		log.Println(query)
		log.Println(err)
		return err
	}
	defer rows.Close()
	return nil
}

func (t *TableInfo) UpdateOrInsert(record AssRow) (int, error) {
	id := -1
	for key, element := range record {
		if key == "id" {
			sid := fmt.Sprintf("%v", element)
			id, _ = strconv.Atoi(sid)
		}
	}
	if id == -1 {
		return t.Insert(record)
	} else {
		t.Update(record)
		return id, nil
	}
}

func removeLastChar(s string) string {
	r := []rune(s)
	return string(r[:len(r)-1])
}

func (ar *AssRow) GetString(column string) string {
	str := fmt.Sprintf("%v", (*ar)[column])
	return str
}

func (ar *AssRow) GetInt(column string) int {
	str := fmt.Sprintf("%v", (*ar)[column])
	val, _ := strconv.Atoi(str)
	return val
}

func (ar *AssRow) GetFloat(column string) float64 {
	str := fmt.Sprintf("%v", (*ar)[column])
	val, _ := strconv.ParseFloat(str, 64)
	return val
}

func Quote(str string) string {
	return pq.QuoteLiteral(str)
}

func (db *Db) SaveSchema(generatedFilename string) error {
	schema, err := db.GetSchema()
	if err != nil {
		log.Println(err)
		return err
	}
	//	file, _ := json.Marshal(schema)
	file, _ := json.MarshalIndent(schema, "", " ")
	_ = ioutil.WriteFile(generatedFilename, file, 0644)
	return nil
}

func buildLinks(schema []TableInfo) []Link {
	var links []Link
	for _, ti := range schema {
		fmt.Println(ti.Name)
		for column, _ := range ti.Columns {
			if strings.HasSuffix(column, "_id") {
				tokens := strings.Split(column, "_")
				linkedtable := tokens[len(tokens)-2]
				var link Link
				link.Source = ti.Name
				link.Destination = linkedtable
				links = append(links, link)
			}
		}
	}
	return links
}

func (db *Db) GenerateTemplate(templateFilename string, generatedFilename string) error {
	schema, err := db.GetSchema()
	if err != nil {
		log.Println(err)
		return err
	}
	links := buildLinks(schema)
	data := struct {
		Tbl []TableInfo
		Lnk []Link
	}{
		schema,
		links,
	}

	t, err := template.ParseFiles(templateFilename)
	if err != nil {
		log.Println(err)
		return err
	}
	f, err := os.Create(generatedFilename)
	if err != nil {
		log.Println("create file: ", err)
		return err
	}
	err = t.Execute(f, data)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (db *Db) GenerateTableTemplates(templateFilename string, outputFolder string, extension string) error {
	schema, err := db.GetSchema()
	if err != nil {
		log.Println(err)
		return err
	}
	for _, ti := range schema {

		t, err := template.ParseFiles(templateFilename)
		if err != nil {
			log.Println(err)
			return err
		}
		f, err := os.Create(outputFolder + string(os.PathSeparator) + ti.Name + "." + extension)
		if err != nil {
			log.Println("create file: ", err)
			return err
		}
		err = t.Execute(f, ti)
		if err != nil {
			log.Println(err)
			return err
		}
	}
	return nil
}
