# sqlmigr
**simple sql migration manager**

&nbsp;&nbsp;&nbsp;&nbsp;Migration file name format: *\<DATE>\<SEPARATOR>\<ACTION>.sql*  
Base script directory is set at construction time.
Date format, separation character, subdirectories for up/down migrations can be changed from default values when constructing a migration object.  
&nbsp;&nbsp;&nbsp;&nbsp;To get a list of all scripts for a particular date use **NewFileList(fromDate time.Time, mgType MigType)**. It also accepts a migration type parameter. The return list is sorted up or down according to migration type specified.  
&nbsp;&nbsp;&nbsp;&nbsp;Use **Up()** and **Down()** methods of the migration object to get the next or the previous migration. After applying the sql script fix the last migration with **SetLastMigrFileName(string)** method passing the last applied migration file name without path.

#### Usage:
```
import(
    "fmt"

    "github.com/dronm/sqlmigr"
)

    dir := "projectDir" //base migration directory
    m := sqlmigr.NewMigrator(dir)
    //change default values:
	//	m.UpDir
	//	m.DownDir
	//	m.DateFormat
	//	m.FilePartSep
	//	m.ScriptExt
	//	m.PosFileName
    

    mgFile, _ := m.Up()
    fmt.Println("next up migration is", mgFile.Name)

    mgFile, _ := m.Down()
    fmt.Println("next down migration is", mgFile.Name)

    scr := m.GetMigrFullFileName(MG_UP, mgFile.Name)
    fmt.Println("run SQL from file", scr)

    //to change last migration
    _ = m.SetLastMigrFileName(mgFile.Name)
```
