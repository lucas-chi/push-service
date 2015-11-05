
package process

import (
	"fmt"
	"io/ioutil"
	"os"
	/*
		"os/user"
		"strconv"
		"strings"
		"syscall"
	*/)

const (
	defaultUser  = "nobody"
	defaultGroup = "nobody"
)

// Init create pid file, set working dir, setgid and setuid.
func Init(userGroup, dir, pidFile string) error {
	// change working dir
	if err := os.Chdir(dir); err != nil {
		return err
	}
	// create pid file
	if err := ioutil.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0644); err != nil {
		return err
	}
	// TODO this can't cross all thread
	/*
		// setuid and setgid
		ug := strings.SplitN(userGroup, " ", 2)
		usr := defaultUser
		grp := defaultGroup
		if len(ug) == 0 {
			// default user and group (nobody)
		} else if len(ug) == 1 {
			usr = ug[0]
			grp = ""
		} else if len(ug) == 2 {
			usr = ug[0]
			grp = ug[1]
		}
		uid := 0
		gid := 0
		ui, err := user.Lookup(usr)
		if err != nil {
			return err
		}
		uid, _ = strconv.Atoi(ui.Uid)
		// group no set
		if grp == "" {
			gid, _ = strconv.Atoi(ui.Gid)
		} else {
			// use user's group instread
			// TODO LookupGroup
			gid, _ = strconv.Atoi(ui.Gid)
		}
			if err := syscall.Setgid(gid); err != nil {
				return err
			}
			if err := syscall.Setuid(uid); err != nil {
				return err
			}
	*/
	return nil
}
