package sqlmigr

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func parseCorrectDate(s string, f string) time.Time {
	d, _ := time.Parse(f, s)
	return d
}

func TestNewMigrFile(t *testing.T) {
	tests := []MigrFile{
		{"20240701105959_act1.sql", parseCorrectDate("20240701105959", DEF_FILE_DATE_FORMAT), "act1"},
		{"20200701105959_act2.sql", parseCorrectDate("20200701105959", DEF_FILE_DATE_FORMAT), "act2"},
		{"20200602235959_act3.sql", parseCorrectDate("20200602235959", DEF_FILE_DATE_FORMAT), "act3"},
	}
	for _, tt := range tests {
		fl, err := NewMigrFile(tt.Name, DEF_FILE_DATE_FORMAT, DEF_FILE_PART_SEP)
		if err != nil {
			t.Fatalf("NewMigrFile() failed: %v", err)
		}
		if !fl.Pos.Equal(tt.Pos) {
			t.Fatalf("Pos expected to be %s, got %s", tt.Pos.Format(DEF_FILE_DATE_FORMAT), fl.Pos.Format(DEF_FILE_DATE_FORMAT))
		}
		if fl.Action != tt.Action {
			t.Fatalf("Action expected to be %s, got %s", tt.Action, fl.Action)
		}
	}
}

func TestNewMigrator(t *testing.T) {
	tests := []struct {
		Dir string
	}{
		{"dir1"},
		{"/home/u1/dir1"},
	}
	for _, tt := range tests {
		m := NewMigrator(tt.Dir)
		if m.Dir != tt.Dir {
			t.Fatalf("Dir expected to be %s, got %s", tt.Dir, m.Dir)
		}
	}
}

func TestMigUpDir(t *testing.T) {
	tests := []struct {
		Dir string
	}{
		{"dir1"},
		{"/home/u1/dir1"},
	}
	for _, tt := range tests {
		m := NewMigrator(tt.Dir)
		exp := filepath.Join(tt.Dir, DEF_UP_DIR)
		if m.GetUpDir() != exp {
			t.Fatalf("Up Dir expected to be %s, got %s", exp, m.Dir)
		}
	}
}

func TestMigDownDir(t *testing.T) {
	tests := []struct {
		Dir string
	}{
		{"dir1"},
		{"/home/u1/dir1"},
	}
	for _, tt := range tests {
		m := NewMigrator(tt.Dir)
		exp := filepath.Join(tt.Dir, DEF_DOWN_DIR)
		if m.GetDownDir() != exp {
			t.Fatalf("Down Dir expected to be %s, got %s", exp, m.Dir)
		}
	}
}

func TestLastMigrFileName(t *testing.T) {
	tests := []struct {
		FileName string
	}{
		{"mig1"},
		{"mig2"},
		{"mig3"},
	}

	//base directory
	baseDir, err := os.MkdirTemp(os.TempDir(), "sqlmigr")
	if err != nil {
		t.Fatalf("os.MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(baseDir) // clean up

	m := NewMigrator(baseDir)

	for _, tt := range tests {
		if err := m.SetLastMigrFileName(tt.FileName); err != nil {
			t.Fatalf("m.SetLastMigrFileName() failed: %v", err)
		}
		got, err := m.GetLastMigrFileName()
		if err != nil {
			t.Fatalf("m.GetLastMigrFileName() failed: %v", err)
		}
		if got != tt.FileName {
			t.Fatalf("current migr expected to be %s, got %s", tt.FileName, got)
		}
	}
}

func TestLastMigrFile(t *testing.T) {
	tests := []MigrFile{
		{"20240101000000_act1.sql", parseCorrectDate("20240101000000", DEF_FILE_DATE_FORMAT), "act1"},
		{"20241231235959_act2.sql", parseCorrectDate("20241231235959", DEF_FILE_DATE_FORMAT), "act2"},
		{"20200101100059_act3.sql", parseCorrectDate("20200101100059", DEF_FILE_DATE_FORMAT), "act3"},
	}

	//base directory
	baseDir, err := os.MkdirTemp(os.TempDir(), "sqlmigr")
	if err != nil {
		t.Fatalf("os.MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(baseDir) // clean up

	m := NewMigrator(baseDir)

	for _, tt := range tests {
		if err := m.SetLastMigrFileName(tt.Name); err != nil {
			t.Fatalf("SetLastMigrFileName failed: %v", err)
		}
		gotFile, err := m.GetLastMigrFile()
		if err != nil {
			t.Fatalf("GetLastMigrFile failed: %v", err)
		}
		if gotFile.Name != tt.Name {
			t.Fatalf("Migration file name expected to be: %s, got %s", tt.Name, gotFile.Name)
		}
		if gotFile.Action != tt.Action {
			t.Fatalf("Migration file action expected to be: %s, got %s", tt.Action, gotFile.Action)
		}
		if !gotFile.Pos.Equal(tt.Pos) {
			t.Fatalf("Migration file pos expected to be: %s, got %s", tt.Pos.Format(m.DateFormat), tt.Pos.Format(m.DateFormat))
		}

	}
}

func TestAdd(t *testing.T) {
	type AddStruct struct {
		MigrFile
		MigrType MigType
		FileCont []byte
	}
	tests := []AddStruct{
		{MigrFile{"20240101000000_act1.sql", parseCorrectDate("20240101000000", DEF_FILE_DATE_FORMAT), "act1"}, MG_UP, []byte{}},
		{MigrFile{"20241231235959_act2.sql", parseCorrectDate("20241231235959", DEF_FILE_DATE_FORMAT), "act2"}, MG_UP, []byte{}},
		{MigrFile{"20200101100058_act3.sql", parseCorrectDate("20200101100058", DEF_FILE_DATE_FORMAT), "act3"}, MG_UP, []byte{}},
	}

	//base directory
	baseDir, err := os.MkdirTemp(os.TempDir(), "sqlmigr")
	if err != nil {
		t.Fatalf("os.MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(baseDir) // clean up

	m := NewMigrator(baseDir)

	if err := os.Mkdir(filepath.Join(baseDir, m.UpDir), 0777); err != nil {
		t.Fatalf("os.Mkdir() failed: %v", err)
	}
	if err := os.Mkdir(filepath.Join(baseDir, m.DownDir), 0777); err != nil {
		t.Fatalf("os.Mkdir() failed: %v", err)
	}

	for _, tt := range tests {
		if err := m.Add(tt.Pos, tt.Action, tt.MigrType, tt.FileCont); err != nil {
			t.Fatalf("m.Add() failed: %v", err)
		}

		mgDir := ""
		if tt.MigrType == MG_UP {
			mgDir = m.UpDir
		} else {
			mgDir = m.DownDir
		}
		mgFile := filepath.Join(baseDir, mgDir, tt.Name)
		fInfo, err := os.Stat(mgFile)
		if err != nil {
			t.Fatalf("os.Stat() failed: %v", err)
		}
		exp := int64(len(tt.FileCont))
		got := fInfo.Size()
		if got != exp {
			t.Fatalf("migration file length expected to be %d, got %d", exp, got)
		}
	}
}

func TestUp(t *testing.T) {
	type AddStruct struct {
		MigrFile
		NextIndex int
	}
	tests := []AddStruct{
		{MigrFile{"20240101000000_act3.sql", parseCorrectDate("20240101000000", DEF_FILE_DATE_FORMAT), "act3"}, 1},
		{MigrFile{"20241231235959_act4.sql", parseCorrectDate("20241231235959", DEF_FILE_DATE_FORMAT), "act4"}, -1},
		{MigrFile{"20200101100058_act2.sql", parseCorrectDate("20200101100058", DEF_FILE_DATE_FORMAT), "act2"}, 0},
		{MigrFile{"20190101100058_act1.sql", parseCorrectDate("20190101100058", DEF_FILE_DATE_FORMAT), "act1"}, 2},
	}

	//base directory
	baseDir, err := os.MkdirTemp(os.TempDir(), "sqlmigr")
	if err != nil {
		t.Fatalf("os.MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(baseDir) // clean up

	m := NewMigrator(baseDir)

	if err := os.Mkdir(filepath.Join(baseDir, m.UpDir), 0777); err != nil {
		t.Fatalf("os.Mkdir() failed: %v", err)
	}
	if err := os.Mkdir(filepath.Join(baseDir, m.DownDir), 0777); err != nil {
		t.Fatalf("os.Mkdir() failed: %v", err)
	}

	//create all files
	for _, tt := range tests {
		fullName := m.GetMigrFullFileName(MG_UP, tt.Name)
		if err := os.WriteFile(fullName, []byte{}, NEW_FILE_PERM); err != nil {
			t.Fatalf("os.WriteFile() failed: %v", err)
		}
	}

	for _, tt := range tests {
		if err := m.SetLastMigrFileName(tt.Name); err != nil {
			t.Fatalf("m.SetLastMigrFileName() failed: %v", err)
		}
		mgFile, err := m.Up()
		if err != nil && !errors.Is(err, ErNoMigration) {
			t.Fatalf("m.Up failed %v", err)
		}

		if tt.NextIndex == -1 && err == nil {
			t.Fatalf("expected no more migrations, found index: %d", tt.NextIndex)
		}

		if tt.NextIndex == -1 && err != nil {
			continue //no more migrations
		}

		expMgFile := tests[tt.NextIndex]
		if expMgFile.Name != mgFile.Name {
			t.Fatalf("expected up migration %s, got %s", expMgFile.Name, mgFile.Name)
		}
	}

}

func TestDown(t *testing.T) {
	type AddStruct struct {
		MigrFile
		NextIndex int
	}
	tests := []AddStruct{
		{MigrFile{"20240101000000_act3.sql", parseCorrectDate("20240101000000", DEF_FILE_DATE_FORMAT), "act3"}, 0},
		{MigrFile{"20241231235959_act4.sql", parseCorrectDate("20241231235959", DEF_FILE_DATE_FORMAT), "act4"}, 1},
		{MigrFile{"20200101100058_act2.sql", parseCorrectDate("20200101100058", DEF_FILE_DATE_FORMAT), "act2"}, 2},
		{MigrFile{"20190101100058_act1.sql", parseCorrectDate("20190101100058", DEF_FILE_DATE_FORMAT), "act1"}, 3},
	}

	//base directory
	baseDir, err := os.MkdirTemp(os.TempDir(), "sqlmigr")
	if err != nil {
		t.Fatalf("os.MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(baseDir) // clean up

	m := NewMigrator(baseDir)

	if err := os.Mkdir(filepath.Join(baseDir, m.UpDir), 0777); err != nil {
		t.Fatalf("os.Mkdir() failed: %v", err)
	}
	if err := os.Mkdir(filepath.Join(baseDir, m.DownDir), 0777); err != nil {
		t.Fatalf("os.Mkdir() failed: %v", err)
	}

	//create all files
	for _, tt := range tests {
		fullName := m.GetMigrFullFileName(MG_DOWN, tt.Name)
		if err := os.WriteFile(fullName, []byte{}, NEW_FILE_PERM); err != nil {
			t.Fatalf("os.WriteFile() failed: %v", err)
		}
	}

	for i, tt := range tests {
		if err := m.SetLastMigrFileName(tt.Name); err != nil {
			t.Fatalf("m.SetLastMigrFileName() failed: %v", err)
		}
		mgFile, err := m.Down()
		if err != nil && !errors.Is(err, ErNoMigration) {
			t.Fatalf("m.Down failed %v", err)
		}

		if tt.NextIndex == -1 && err == nil {
			t.Fatalf("expected no more migrations, found index: %d", tt.NextIndex)
		}

		if tt.NextIndex == -1 && err != nil {
			continue //no more migrations
		}

		expMgFile := tests[tt.NextIndex]
		if expMgFile.Name != mgFile.Name {
			t.Fatalf("at index %d expected down migration %s, got %s", i, expMgFile.Name, mgFile.Name)
		}
	}

}

func TestUpFileList(t *testing.T) {
	type AddStruct struct {
		MigrFile
		CorrectIndex int
	}
	tests := []AddStruct{
		{MigrFile{"20240101000000_act3.sql", parseCorrectDate("20240101000000", DEF_FILE_DATE_FORMAT), "act3"}, 2},
		{MigrFile{"20241231235959_act4.sql", parseCorrectDate("20241231235959", DEF_FILE_DATE_FORMAT), "act4"}, -1},
		{MigrFile{"20200101100058_act2.sql", parseCorrectDate("20200101100058", DEF_FILE_DATE_FORMAT), "act2"}, 1},
		{MigrFile{"20190101100058_act1.sql", parseCorrectDate("20190101100058", DEF_FILE_DATE_FORMAT), "act1"}, 0},
	}

	//base directory
	baseDir, err := os.MkdirTemp(os.TempDir(), "sqlmigr")
	if err != nil {
		t.Fatalf("os.MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(baseDir) // clean up

	m := NewMigrator(baseDir)

	if err := os.Mkdir(filepath.Join(baseDir, m.UpDir), 0777); err != nil {
		t.Fatalf("os.Mkdir() failed: %v", err)
	}

	//create all files
	for _, tt := range tests {
		fullName := m.GetMigrFullFileName(MG_UP, tt.Name)
		if err := os.WriteFile(fullName, []byte{}, NEW_FILE_PERM); err != nil {
			t.Fatalf("os.WriteFile() failed: %v", err)
		}
	}
	files, err := m.NewFileList(time.Time{}, MG_UP)
	if err != nil {
		t.Fatalf("m.NewFileList failed %v", err)
	}

	for i, f := range files {
		found := false
		for _, tt := range tests {
			if tt.Name == f.Name {
				if tt.CorrectIndex == -1 && i+1 != len(files) {
					t.Fatalf("index %d  name=%s expected the last migration", i, tt.Name)
				}
				if tt.CorrectIndex >= 0 && tt.CorrectIndex != i {
					t.Fatalf("index %d  name=%s expected to be at index %d", i, tt.Name, tt.CorrectIndex)
				}
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("file with name %s not found", f.Name)
		}
	}
}

func TestDownFileList(t *testing.T) {
	type AddStruct struct {
		MigrFile
		CorrectIndex int
	}
	tests := []AddStruct{
		{MigrFile{"20240101000000_act3.sql", parseCorrectDate("20240101000000", DEF_FILE_DATE_FORMAT), "act3"}, 2},
		{MigrFile{"20241231235959_act4.sql", parseCorrectDate("20241231235959", DEF_FILE_DATE_FORMAT), "act4"}, 3},
		{MigrFile{"20200101100058_act2.sql", parseCorrectDate("20200101100058", DEF_FILE_DATE_FORMAT), "act2"}, 1},
		{MigrFile{"20190101100058_act1.sql", parseCorrectDate("20190101100058", DEF_FILE_DATE_FORMAT), "act1"}, 0},
	}

	//base directory
	baseDir, err := os.MkdirTemp(os.TempDir(), "sqlmigr")
	if err != nil {
		t.Fatalf("os.MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(baseDir) // clean up

	m := NewMigrator(baseDir)

	if err := os.Mkdir(filepath.Join(baseDir, m.UpDir), 0777); err != nil {
		t.Fatalf("os.Mkdir() failed: %v", err)
	}

	//create all files
	for _, tt := range tests {
		fullName := m.GetMigrFullFileName(MG_UP, tt.Name)
		if err := os.WriteFile(fullName, []byte{}, NEW_FILE_PERM); err != nil {
			t.Fatalf("os.WriteFile() failed: %v", err)
		}
	}
	files, err := m.NewFileList(time.Time{}, MG_UP)
	if err != nil {
		t.Fatalf("m.NewFileList failed %v", err)
	}
	// for _, f := range files {
	// 	fmt.Println(f)
	// }
	for i, f := range files {
		found := false
		for _, tt := range tests {
			if tt.Name == f.Name {
				if tt.CorrectIndex != i {
					t.Fatalf("index %d  name=%s expected to be at index %d", i, tt.Name, tt.CorrectIndex)
				}
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("file with name %s not found", f.Name)
		}

	}
}
