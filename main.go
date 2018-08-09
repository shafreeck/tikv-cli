// Copyright © 2018 Shafreeck Sea <shafreeck@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"log"
	"os"
	"strings"

	"github.com/c-bata/go-prompt"
	"github.com/spf13/cobra"
)

type Options struct {
	Url string
}

type command struct {
	cli *TikvClient

	scanOpts struct {
		limit int
	}
}

func (c *command) get(args []string) {
	if len(args) == 0 {
		log.Println("key is required")
	}
	for i := range args {
		key := args[i]
		val, err := c.cli.Get([]byte(key))
		if err != nil {
			log.Fatalln(err)
		}
		log.Println(string(val))
	}
}
func (c *command) set(args []string) {
	if len(args) != 2 {
		return
	}
	key, val := args[0], args[1]
	err := c.cli.Set([]byte(key), []byte(val))
	if err != nil {
		log.Fatalln(err)
	}
}

func (c *command) delete(args []string) {
	if len(args) == 0 {
		log.Println("key is required")
	}
	for i := range args {
		key := args[i]
		if err := c.cli.Delete([]byte(key)); err != nil {
			log.Fatalln(err)
		}
	}
}

func (c *command) scan(args []string) {
	if len(args) == 0 {
		log.Fatalln("begin key is required")
	}

	begin := args[0]
	c.cli.Scan([]byte(begin), c.scanOpts.limit, func(key, val []byte) {
		log.Println(string(key), string(val))
	})
}

func cobraWapper(f func(args []string)) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		f(args)
	}
}

func promptCompleter(d prompt.Document) []prompt.Suggest {
	s := []prompt.Suggest{
		{Text: "get", Description: "get key1 key2 key3..."},
		{Text: "set", Description: "set key val"},
		{Text: "delete", Description: "delete key"},
		{Text: "scan", Description: "scan begin"},
	}
	return prompt.FilterHasPrefix(s, d.GetWordBeforeCursor(), true)
}

func processLine(c *command, line string) {
	args := strings.Split(line, " ")
	if len(args) == 0 {
		return
	}
	cmd := args[0]
	switch cmd {
	case "get":
		c.get(args[1:])
	case "set":
		c.set(args[1:])
	case "delete":
		c.delete(args[1:])
	case "scan":
		fs := (&cobra.Command{}).Flags()
		fs.IntVarP(&c.scanOpts.limit, "limit", "n", 10, "number of values to be scanned")
		if err := fs.Parse(args[1:]); err != nil {
			log.Println(err)
		}
		c.scan(fs.Args())
	default:
		log.Println("unkown command", cmd)
	}
}

func main() {
	opts := &Options{}
	c := &command{}

	//log.SetFlags(0)

	cmd := cobra.Command{Use: "tikv"}
	cmd.PersistentFlags().StringVarP(&opts.Url, "url", "u", "", "tikv://etcd-node1:port,etcd-node2:port?cluster=1&disableGC=false")
	cmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		cli, err := Dial(opts.Url)
		if err != nil {
			log.Fatalln(err)
		}
		c.cli = cli
	}
	cmd.Run = func(cmd *cobra.Command, args []string) {
		for {
			line := prompt.Input("> ", promptCompleter, prompt.OptionAddKeyBind(prompt.KeyBind{Key: prompt.ControlD, Fn: func(*prompt.Buffer) { os.Exit(0) }}))
			if line == "exit" || line == "quit" {
				os.Exit(0)
			}
			processLine(c, line)
		}
	}

	get := &cobra.Command{Use: "get <key>", Run: cobraWapper(c.get)}
	cmd.AddCommand(get)

	set := &cobra.Command{Use: "set <key> <val>", Run: cobraWapper(c.set)}
	cmd.AddCommand(set)

	scan := &cobra.Command{Use: "scan <begin>", Run: cobraWapper(c.scan)}
	scan.Flags().IntVarP(&c.scanOpts.limit, "limit", "n", 10, "number of values to be scanned")
	cmd.AddCommand(scan)

	delete := &cobra.Command{Use: "delete <key>", Run: cobraWapper(c.delete)}
	cmd.AddCommand(delete)

	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
