// Package sqlmigr manages sql migration files.
package sqlmigr

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	DEF_FILE_DATE_FORMAT = "20060102150405"
	DEF_FILE_PART_SEP    = "_"
	DEF_DOWN_DIR         = "down"
	DEF_UP_DIR           = "up"
	DEF_SCRIPT_EXT       = ".sql"
	DEF_POS_FILE_NAME    = "last_mig.pos"

	NEW_FILE_PERM = 0644
)

// MigType stands for two migraions: up and down.
type MigType int

func (m MigType) String() string {
	if m == MG_UP {
		return "up"
	} else {
		return "down"
	}
}

const (
	MG_UP MigType = iota
	MG_DOWN
)

// ErNoMigration is used for returning migration absence.
var ErNoMigration = errors.New("no migrations")

// MigrFile is a migration script structure.
type MigrFile struct {
	Name   string
	Pos    time.Time
	Action string
}

// NewMigrFile constructs MigrFile object from file name.
// Date format and file parts separator are olso provided.
// Migration file name format: <DATE><SEPARATOR><ACTION>.sql
// Where date is in format  dateFormat. Separator is partSep.
func NewMigrFile(migrFileName, dateFormat, partSep string) (*MigrFile, error) {
	fileName := migrFileName[:len(migrFileName)-len(filepath.Ext(migrFileName))] //strip extension

	fileParts := strings.Split(fileName, partSep)
	if len(fileParts) == 0 {
		return nil, fmt.Errorf("unknown file name format")
	}

	//date part
	d, err := time.Parse(dateFormat, fileParts[0])
	if err != nil {
		return nil, err
	}

	migrFile := MigrFile{Name: migrFileName, Pos: d}
	if len(fileParts) >= 2 {
		migrFile.Action = fileParts[1]
	}

	return &migrFile, nil
}

// MigrFileList implements sor.Interface
type MigrFileList []*MigrFile

func (f MigrFileList) Len() int {
	return len(f)
}

func (f MigrFileList) Less(i, j int) bool {
	return f[i].Pos.Before(f[j].Pos)
}

func (f MigrFileList) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

type Migrator struct {
	//default values
	Dir         string // base directory
	UpDir       string
	DownDir     string
	DateFormat  string
	FilePartSep string
	ScriptExt   string // migration script file extension .sql is default value
	PosFileName string // name of the last position file.

	FileList MigrFileList
}

func NewMigrator(dir string) *Migrator {
	return &Migrator{Dir: dir,
		ScriptExt:   DEF_SCRIPT_EXT,
		UpDir:       DEF_UP_DIR,
		DownDir:     DEF_DOWN_DIR,
		DateFormat:  DEF_FILE_DATE_FORMAT,
		FilePartSep: DEF_FILE_PART_SEP,
		PosFileName: DEF_POS_FILE_NAME,
	}
}

func (m *Migrator) GetDownDir() string {
	return filepath.Join(m.Dir, m.DownDir)
}

func (m *Migrator) GetUpDir() string {
	return filepath.Join(m.Dir, m.UpDir)
}

func (m *Migrator) GetLastMigrFileName() (string, error) {
	fileName := filepath.Join(m.Dir, m.PosFileName)
	if _, err := os.Stat(fileName); err != nil {
		return "", err
	}
	fData, err := os.ReadFile(fileName)
	if err != nil {
		return "", err
	}
	return string(fData), nil
}

// curMigrFileName is a migration file name, no dir.
func (m *Migrator) SetLastMigrFileName(curMigrFileName string) error {
	if err := os.WriteFile(filepath.Join(m.Dir, m.PosFileName), []byte(curMigrFileName), NEW_FILE_PERM); err != nil {
		return err
	}

	return nil
}

func (m *Migrator) GetLastMigrFile() (*MigrFile, error) {
	fName, err := m.GetLastMigrFileName()
	if err != nil {
		return nil, err
	}

	f, err := NewMigrFile(fName, m.DateFormat, m.FilePartSep)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// NextMigration returns next migration of the given type.
func (m *Migrator) NextMigration(mgType MigType) (*MigrFile, error) {
	mgFile, err := m.GetLastMigrFile()
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return nil, ErNoMigration
	} else if err != nil {
		return nil, err
	}

	files, err := m.NewFileList(mgFile.Pos, mgType)
	if err != nil {
		return nil, err
	}
	if len(files) > 0 {
		return files[0], nil
	}
	return nil, ErNoMigration
}

// Down returns previous migration.
// If no migration found then ErNoMigration is returned.
func (m *Migrator) Down() (*MigrFile, error) {
	return m.NextMigration(MG_DOWN)
}

// Next returns next up migration file.
// If no migration found then ErNoMigration is returned.
func (m *Migrator) Up() (*MigrFile, error) {
	return m.NextMigration(MG_UP)
}

// Add creates a new migration of type mgType with time t and action act.
func (m *Migrator) Add(t time.Time, act string, mgType MigType, fileContent []byte) error {
	fName := m.GetMigrFileName(t, act)
	fullName := m.GetMigrFullFileName(mgType, fName)
	if err := os.WriteFile(fullName, fileContent, NEW_FILE_PERM); err != nil {
		return err
	}

	return nil
}

// GetMigrDir returns full dir for the given migration.
func (m *Migrator) GetMigrFullFileName(mgType MigType, fName string) string {
	mgDir := ""
	if mgType == MG_UP {
		mgDir = m.UpDir
	} else {
		mgDir = m.DownDir
	}
	return filepath.Join(m.Dir, mgDir, fName)
}

// GetMigrFileName returns migration file name constructed based on Migrator parameters.
func (m *Migrator) GetMigrFileName(t time.Time, act string) string {
	return t.Format(m.DateFormat) + m.FilePartSep + act + m.ScriptExt
}

// NewFileList returns migration files sorted by dates according to selected migration type.
func (m *Migrator) NewFileList(fromDate time.Time, mgType MigType) (MigrFileList, error) {
	var files MigrFileList

	mgDir := m.GetMigrFullFileName(mgType, "")

	aFiles, err := os.ReadDir(mgDir)
	if err != nil {
		return nil, err
	}

	empty_time := time.Time{}
	for _, info := range aFiles {
		if info.IsDir() {
			continue
		}
		// filepath.Join(mgDir, info.Name())
		mgFile, err := NewMigrFile(info.Name(), m.DateFormat, m.FilePartSep)
		if err != nil {
			return nil, err
		}

		if fromDate != empty_time &&
			(mgType == MG_UP && (mgFile.Pos.Equal(fromDate) || mgFile.Pos.Before(fromDate))) ||
			(mgType == MG_DOWN && mgFile.Pos.After(fromDate)) {
			continue
		}

		files = append(files, mgFile)
	}

	if mgType == MG_UP {
		sort.Sort(files)
	} else {
		sort.Sort(sort.Reverse(files))
	}

	// fmt.Println("got file list for", mgType, "for date", fromDate)
	// for _, f := range files {
	// 	fmt.Println(f)
	// }
	return files, nil
}
