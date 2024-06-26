package sqldb

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"os"
	"reflect"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/rs/zerolog"
)

var log zerolog.Logger

type Db struct {
	Driver     string
	Url        string
	LogQueries bool
	conn       *sql.DB
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
		log.Error().Msg(err.Error())
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

// GetAssociativeArray : Provide table data as an associative array
func (t *TableInfo) GetAssociativeArray(columns []string, restriction string, sortkeys []string, dir string) ([]AssRow, error) {
	return t.db.QueryAssociativeArray(t.buildSelect("", columns, restriction, sortkeys, dir))
}

// QueryAssociativeArray : Provide query result as an associative array
func (db *Db) QueryAssociativeArray(query string) (Rows, error) {
	if db.LogQueries {
		log.Info().Msg(query)
	}
	rows, err := db.conn.Query(query)
	if err != nil {
		log.Error().Msg(err.Error())
		log.Error().Msg(query)
		return nil, err
	}
	defer rows.Close()
	// get rows
	results := Rows{}
	cols, err := rows.Columns()
	if err != nil {
		log.Error().Msg(err.Error())
		log.Error().Msg(query)
		return nil, err
	}
	// make types map
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	columnType := make(map[string]string)
	for _, colType := range columnTypes {
		columnType[colType.Name()] = colType.DatabaseTypeName()
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
		err = rows.Scan(columnPointers...)
		if err != nil {
			return nil, err
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make(AssRow)
		for i, colName := range cols {
			//fmt.Println(colName)
			val := columnPointers[i].(*interface{})
			if db.Driver == "mysql" {
				if (*val) == nil {
					m[colName] = nil
				} else {
					switch columnType[colName] {
					case "INT", "BIGINT":
						i, err := strconv.ParseInt(fmt.Sprintf("%s", *val), 10, 64)
						if err != nil {
							return nil, err
						}
						m[colName] = i
					case "UNSIGNED BIGINT", "UNSIGNED INT":
						u, err := strconv.ParseUint(fmt.Sprintf("%s", *val), 10, 64)
						if err != nil {
							return nil, err
						}
						m[colName] = u
					case "FLOAT":
						f, err := strconv.ParseFloat(fmt.Sprintf("%s", *val), 64)
						if err != nil {
							return nil, err
						}
						m[colName] = f
					case "TINYINT":
						i, err := strconv.ParseInt(fmt.Sprintf("%s", *val), 10, 64)
						if err != nil {
							return nil, err
						}
						if i == 1 {
							m[colName] = true
						} else {
							m[colName] = false
						}

					case "VARCHAR", "TEXT", "TIMESTAMP", "VARBINARY":
						m[colName] = fmt.Sprintf("%s", *val)
					default:
						if reflect.ValueOf(val).IsNil() {
							m[colName] = nil
						} else {
							fmt.Printf("Unknow type : %s (%s)\n", columnType[colName], query)
							m[colName] = fmt.Sprintf("%v", *val)
						}
					}
				}
			}
			if db.Driver == "postgres" || db.Driver == "sqlserver" {
				m[colName] = *val
			}
		}
		results = append(results, m)
	}
	return results, nil
}

// GetSchema : Provide table schema as an associative array
func (t *TableInfo) GetSchema() (*TableInfo, error) {
	pgSchema := "SELECT column_name :: varchar as name, REPLACE(REPLACE(data_type,'character varying','varchar'),'character','char') || COALESCE('(' || character_maximum_length || ')', '') as type, col_description('public." + t.Name + "'::regclass, ordinal_position) as comment  from INFORMATION_SCHEMA.COLUMNS where table_name ='" + t.Name + "';"
	mySchema := "SELECT COLUMN_NAME as name, CONCAT(DATA_TYPE, COALESCE(CONCAT('(' , CHARACTER_MAXIMUM_LENGTH, ')'), '')) as type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + t.Name + "';"
	// nob
	msSchema := "SELECT COLUMN_NAME as name, CONCAT(DATA_TYPE, COALESCE(CONCAT('(' , CHARACTER_MAXIMUM_LENGTH, ')'), '')) as type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + t.Name + "';"

	var schemaQuery string
	var ti TableInfo
	ti.Name = t.Name
	ti.db = t.db
	if t.db.Driver == "postgres" {
		schemaQuery = pgSchema
	}
	if t.db.Driver == "mysql" {
		schemaQuery = mySchema
	}
	// nob requête identique
	if t.db.Driver == "sqlserver" {
		schemaQuery = msSchema
	}
	cols, err := t.db.QueryAssociativeArray(schemaQuery)
	if err != nil {
		log.Error().Msg(err.Error())
		return nil, err
	}
	ti.Columns = make(map[string]string)
	for _, row := range cols {
		var name, rowtype, comment string
		for key, element := range row {
			if key == "name" {
				name = fmt.Sprintf("%s", element)
			}
			if key == "type" {
				rowtype = fmt.Sprintf("%s", element)
			}
			if key == "comment" {
				comment = fmt.Sprintf("%s", element)
			}
		}
		ti.Columns[name] = rowtype
		if comment != "<nil>" && strings.TrimSpace(comment) != "" {
			ti.Columns[name] = ti.Columns[name] + "|" + comment
		}
	}
	return &ti, nil
}

// GetSchema : Provide full database schema as an associative array
func (db *Db) GetSchema() ([]TableInfo, error) {
	var res []TableInfo
	tables, err := db.ListTables()
	if err != nil {
		log.Error().Msg(err.Error())
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
				log.Error().Msg(err.Error())
				return nil, err
			}
			res = append(res, *fullti)
		}
	}
	return res, nil
}

// GetSchema : Provide database tables list
func (db *Db) ListTables() (Rows, error) {
	if db.Driver == "postgres" {
		return db.pgListTables()
	}
	if db.Driver == "mysql" {
		return db.myListTables()
	}
	// nob requête identique
	if db.Driver == "sqlserver" {
		return db.msListTables()
	}
	return nil, errors.New("no driver")
}

func (db *Db) pgListTables() (Rows, error) {
	return db.QueryAssociativeArray("SELECT table_name :: varchar as name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;")
}

func (db *Db) myListTables() (Rows, error) {
	return db.QueryAssociativeArray("SELECT TABLE_NAME as name FROM information_schema.TABLES WHERE TABLE_TYPE LIKE 'BASE_TABLE';")
}

// nob identical request
func (db *Db) msListTables() (Rows, error) {
	return db.QueryAssociativeArray("SELECT TABLE_NAME as name FROM information_schema.TABLES WHERE TABLE_TYPE LIKE 'BASE_TABLE';")
}

func (db *Db) CreateTable(t TableInfo) error {
	if db.Driver == "postgres" {
		return db.pgCreateTable(t)
	}
	if db.Driver == "mysql" {
		return db.myCreateTable(t)
	}
	// nob requête identique
	if db.Driver == "sqlserver" {
		return db.myCreateTable(t)
	}
	return errors.New("no driver")
}

func (db *Db) pgCreateTable(t TableInfo) error {
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
	if db.LogQueries {
		log.Info().Msg(query)
	}
	_, err := t.db.conn.Query(query)
	if err != nil {
		log.Error().Msg(err.Error())
		return err
	}
	for name, rowtype := range t.Columns {
		desc := strings.Split(fmt.Sprintf("%v", rowtype), "|")
		if len(desc) > 1 {
			query = "COMMENT ON COLUMN " + t.Name + "." + fmt.Sprintf("%v", name) + " IS '" + desc[1] + "'"
			if db.LogQueries {
				log.Info().Msg(query)
			}
			_, err := t.db.conn.Query(query)
			if err != nil {
				log.Error().Msg(err.Error())
				return err
			}
		}

	}
	return nil
}

func (db *Db) myCreateTable(t TableInfo) error {
	t.db = db
	query := "create table " + t.Name + " ( "
	columns := ""
	for name, rowtype := range t.Columns {
		if fmt.Sprintf("%v", name) == "id" {
			columns += fmt.Sprintf("%v", name) + " " + "SERIAL PRIMARY KEY,"
		} else {
			desc := strings.Split(fmt.Sprintf("%v", rowtype), "|")
			columns += fmt.Sprintf("%v", name) + " " + desc[0]
			if len(desc) > 1 {
				columns += " COMMENT " + pq.QuoteLiteral(desc[1])
			}
			columns += ","
		}
	}
	query += columns
	query = query[:len(query)-1] + " )"
	if db.LogQueries {
		log.Info().Msg(query)
	}
	_, err := t.db.conn.Query(query)
	if err != nil {
		log.Error().Msg(err.Error())
		return err
	}
	return nil
}

func (t *TableInfo) DeleteTable() error {
	query := "drop table " + t.Name
	if t.db.LogQueries {
		log.Info().Msg(query)
	}
	_, err := t.db.conn.Query(query)
	if err != nil {
		log.Error().Msg(err.Error())
		return err
	}
	query = "drop sequence if exists sq_" + t.Name
	if t.db.LogQueries {
		log.Info().Msg(query)
	}
	_, err = t.db.conn.Query(query)
	if err != nil {
		log.Error().Msg(err.Error())
		return err
	}
	return nil
}

func (t *TableInfo) AddColumn(name string, sqltype string, comment string) error {
	if t.db.Driver == "postgres" {
		return t.pgAddColumn(name, sqltype, comment)
	}
	if t.db.Driver == "mysql" {
		return t.myAddColumn(name, sqltype, comment)
	}
	return errors.New("no driver")
}

func (t *TableInfo) pgAddColumn(name string, sqltype string, comment string) error {
	query := "alter table " + t.Name + " add " + name + " " + sqltype
	if t.db.LogQueries {
		log.Info().Msg(query)
	}
	rows, err := t.db.conn.Query(query)
	if err != nil {
		log.Error().Msg(err.Error())
		return err
	}
	if strings.TrimSpace(comment) != "" {
		query = "COMMENT ON COLUMN " + t.Name + "." + name + " IS '" + comment + "'"
		if t.db.LogQueries {
			log.Info().Msg(query)
		}
		_, err = t.db.conn.Query(query)
		if err != nil {
			log.Error().Msg(err.Error())
			return err
		}
	}
	defer rows.Close()
	return nil
}

func (t *TableInfo) myAddColumn(name string, sqltype string, comment string) error {
	query := "alter table " + t.Name + " add " + name + " " + sqltype
	if strings.TrimSpace(comment) != "" {
		query += " COMMENT " + pq.QuoteLiteral(comment)
	}
	if t.db.LogQueries {
		log.Info().Msg(query)
	}
	rows, err := t.db.conn.Query(query)
	if err != nil {
		log.Error().Msg(err.Error())
		return err
	}
	defer rows.Close()
	return nil
}

func (t *TableInfo) DeleteColumn(name string) error {
	query := "alter table " + t.Name + " drop " + name
	if t.db.LogQueries {
		log.Info().Msg(query)
	}
	rows, err := t.db.conn.Query(query)
	if err != nil {
		log.Error().Msg(err.Error())
		return err
	}
	defer rows.Close()
	return nil
}

func (db *Db) ImportSchema(filename string) {
	byteValue, _ := os.ReadFile(filename)

	var jsonSource []TableInfo
	err := json.Unmarshal([]byte(byteValue), &jsonSource)
	if err != nil {
		log.Error().Msg(err.Error())
	}
	for _, ti := range jsonSource {
		ti.db = db
		err = db.CreateTable(ti)
		if err != nil {
			log.Error().Msg(err.Error())
		}
	}
}

func (db *Db) ClearImportSchema(filename string) {
	byteValue, _ := os.ReadFile(filename)

	var jsonSource []TableInfo
	json.Unmarshal([]byte(byteValue), &jsonSource)
	for _, ti := range jsonSource {
		ti.db = db
		err := ti.DeleteTable()
		if err != nil {
			log.Error().Msg(err.Error())
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

func (t *TableInfo) Insert(record AssRow) (int64, error) {
	columns := ""
	values := ""
	t, err := t.GetSchema()
	if err != nil {
		log.Error().Msg(err.Error())
		return -1, err
	}
	var id int64

	for key, element := range record {
		columns += key + ","
		values += FormatForSQL(t.Columns[key], element) + ","
	}
	if t.db.Driver == "postgres" {
		query := "INSERT INTO " + t.Name + "(" + removeLastChar(columns) + ") VALUES (" + removeLastChar(values) + ") RETURNING id"
		if t.db.LogQueries {
			log.Info().Msg(query)
		}
		err = t.db.conn.QueryRow(query).Scan(&id)
	}
	if t.db.Driver == "mysql" {
		/*		_, err = t.db.conn.Query("INSERT INTO " + t.Name + "(" + removeLastChar(columns) + ") VALUES (" + removeLastChar(values) + ")")
				if err != nil {
					return id, err
				}
				err = t.db.conn.QueryRow("SELECT LAST_INSERT_ID()").Scan(&id)*/

		query := "INSERT INTO " + t.Name + "(" + removeLastChar(columns) + ") VALUES (" + removeLastChar(values) + ")"
		stmt, err := t.db.conn.Prepare(query)
		if t.db.LogQueries {
			log.Info().Msg(query)
		}
		fmt.Println("INSERT INTO " + t.Name + "(" + removeLastChar(columns) + ") VALUES (" + removeLastChar(values) + ")")
		if err != nil {
			return id, err
		}
		res, err := stmt.Exec()
		if err != nil {
			return id, err
		}
		id, err = res.LastInsertId()
		if err != nil {
			return id, err
		}
	}
	return id, err
}

func (t *TableInfo) Update(record AssRow) error {

	t, err := t.GetSchema()
	if err != nil {
		log.Error().Msg(err.Error())
		return err
	}
	id := ""
	stack := ""

	for key, element := range record {

		if key == "id" {
			id = fmt.Sprintf("%v", element)
		} else {
			stack = stack + " " + key + " = " + FormatForSQL(t.Columns[key], element) + ","
		}

	}
	stack = removeLastChar(stack)
	query := ("UPDATE " + t.Name + " SET " + stack + " WHERE id = " + id)
	if t.db.LogQueries {
		log.Info().Msg(query)
	}
	rows, err := t.db.conn.Query(query)
	if err != nil {
		log.Error().Msg(query)
		log.Error().Msg(err.Error())
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
			break
		}
	}
	query := ("DELETE FROM " + t.Name + " WHERE id = " + id)
	if t.db.LogQueries {
		log.Info().Msg(query)
	}
	rows, err := t.db.conn.Query(query)
	if err != nil {
		log.Error().Msg(query)
		log.Error().Msg(err.Error())
		return err
	}
	defer rows.Close()
	return nil
}

func (t *TableInfo) WildDelete(restriction string) error {
	query := ("DELETE FROM " + t.Name + " WHERE " + restriction)
	if t.db.LogQueries {
		log.Info().Msg(query)
	}
	rows, err := t.db.conn.Query(query)
	if err != nil {
		log.Error().Msg(query)
		log.Error().Msg(err.Error())
		return err
	}
	defer rows.Close()
	return nil
}

func (t *TableInfo) UpdateOrInsert(record AssRow) (int64, error) {
	var id int64
	id = -1
	for key, element := range record {
		if key == "id" {
			sid := fmt.Sprintf("%v", element)
			id, _ = strconv.ParseInt(sid, 10, 64)
			break
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
		log.Error().Msg(err.Error())
		return err
	}
	//	file, _ := json.Marshal(schema)
	file, _ := json.MarshalIndent(schema, "", " ")
	_ = os.WriteFile(generatedFilename, file, 0644)
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

// Generate templates from a schema
func (db *Db) GenerateSchemaTemplate(templateFilename string, generatedFilename string) error {
	schema, err := db.GetSchema()
	if err != nil {
		log.Error().Msg(err.Error())
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
		log.Error().Msg(err.Error())
		return err
	}
	f, err := os.Create(generatedFilename)
	if err != nil {
		log.Error().Msg("create file: " + err.Error())
		return err
	}
	err = t.Execute(f, data)
	if err != nil {
		log.Error().Msg(err.Error())
		return err
	}
	return nil
}

// Generate tables
func (db *Db) GenerateTableTemplates(templateFilename string, outputFolder string, extension string) error {
	schema, err := db.GetSchema()
	if err != nil {
		log.Error().Msg(err.Error())
		return err
	}
	for _, ti := range schema {

		t, err := template.ParseFiles(templateFilename)
		if err != nil {
			log.Error().Msg(err.Error())
			return err
		}
		f, err := os.Create(outputFolder + string(os.PathSeparator) + ti.Name + "." + extension)
		if err != nil {
			log.Error().Msg("create file: " + err.Error())
			return err
		}
		err = t.Execute(f, ti)
		if err != nil {
			log.Error().Msg(err.Error())
			return err
		}
	}
	return nil
}

func FormatForSQL(datatype string, value interface{}) string {
	if value == nil {
		return "NULL"
	}
	strval := fmt.Sprintf("%v", value)
	if !strings.Contains(datatype, "char") && len(strval) == 0 {
		return "NULL"
	}
	if strings.Contains(datatype, "char") || strings.Contains(datatype, "text") || strings.Contains(datatype, "date") || strings.Contains(datatype, "timestamp") {
		return fmt.Sprint(pq.QuoteLiteral(strval))
	}
	return fmt.Sprint(strval)
}

// Build a map based on id from a query result
func (db *Db) BuildIdMap(idxkey string, rows Rows) (map[int64]AssRow, error) {
	ht := make(map[int64]AssRow)
	for _, row := range rows {
		id := row[idxkey].(int64)
		ht[id] = row
	}
	return ht, nil
}
