package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strings"

	. "github.com/hoffmale/csgo"
	"github.com/nsf/termbox-go"
)

// to enable cpu profiling
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

type Action struct {
	Command     string
	Description string
	Execute     func(cmd string)
}

// UIState contains stuff
type UIState struct {
	Actions     []Action
	ParentState *UIState

	Name string

	PrintUI     func()
	HandleInput func(input string)
}

// PrintOptions does stuff
func (uiState *UIState) PrintOptions(xPosMin, yPosMin, xPosMax, yPosMax int, wrap bool) {
	curRow := 0
	for _, action := range uiState.Actions {
		write(xPosMin, yPosMin+curRow, fmt.Sprintf("%s: %s", action.Command, action.Description), false)
		curRow++
	}
}

var database *ColumnStore
var rootUI *UIState
var currentUI *UIState
var isRunning = true

func write(xPos, yPos int, text string, wrapAround bool) {
	curIndex := 0

	for _, charValue := range text {
		termbox.SetCell(xPos+curIndex, yPos, charValue, termbox.ColorWhite, termbox.ColorBlack)
		curIndex++
	}
}

func writeCurrentUIState() {
	curUI := currentUI
	locStr := ""

	for curUI != rootUI {
		locStr = curUI.Name + " > " + locStr
		curUI = curUI.ParentState
	}

	locStr = "> " + locStr
	write(0, 0, locStr, false)
}

func clear() {
	termbox.Clear(termbox.ColorWhite, termbox.ColorBlack)
	_, termHeight := termbox.Size()
	writeCurrentUIState()
	write(0, termHeight-1, "> ", false)
	termbox.SetCursor(2, termHeight-1)
}

func createScrollView(name string, indent int, yMinPos, yMaxPos int, lines []string) {
	scrollState := &UIState{}
	scrollState.ParentState = currentUI
	scrollState.Name = name

	rowOffset := 0

	scrollState.PrintUI = func() {
		for i := 0; i <= yMaxPos-yMinPos; i++ {
			if rowOffset+i >= len(lines) {
				break
			}
			write(indent, i+yMinPos, lines[rowOffset+i], false)
		}
	}

	scrollState.HandleInput = func(command string) {
		oldUIState := currentUI
		currentUI = scrollState.ParentState
		tryParseAction(command)
		currentUI = oldUIState
	}

	currentUI = scrollState
}

func setupUI() {
	rootUI = &UIState{
		Actions: []Action{
			{"list", "lists all tables available", func(ignore string) {
				_, termHeight := termbox.Size()
				curIndex := 0
				clear()
				write(0, 2, "The database contains the following table(s):", false)
				for _, table := range database.Relations {
					write(4, 3+curIndex, table.(Relation).Name, false)
					if curIndex+5 > termHeight {
						break
					}
					curIndex++
				}
				termbox.Flush()
			}},
			{"help", "lists all currently available commands", func(ignore string) {
				_, termHeight := termbox.Size()
				lines := []string{}
				for _, action := range currentUI.Actions {
					lines = append(lines, fmt.Sprintf("%s: %s", action.Command, action.Description))
				}
				createScrollView("help", 4, 4, termHeight-7, lines)
				clear()
				write(0, 2, "The following commands are currently available:", false)
				currentUI.PrintUI()
				termbox.Flush()
			}},
		},
	}
	rootUI.ParentState = rootUI
	rootUI.PrintUI = func() {
		write(0, 1, "hi", false)
	}

	currentUI = rootUI
}

func tryParseAction(command string) bool {
	for _, action := range currentUI.Actions {
		if strings.HasPrefix(command, action.Command) {
			action.Execute(command)
			return true
		}
	}

	return false
}

func main() {
	// enable CPU profiling [BEGIN]
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	// enable CPU profiling [END]
	err := termbox.Init()
	if err != nil {
		panic(err)
	}
	defer termbox.Close()

	setupUI()
	database = &ColumnStore{}
	database.CreateRelation("test1", []AttrInfo{{"testCol", INT, NOCOMP}})

	command := ""
	commandLength := 0
	commandStartIndex := 0
	commandCursorIndex := 0

	termWidth, termHeight := termbox.Size()

	min := func(a, b int) int {
		if a <= b {
			return a
		}
		return b
	}
	max := func(a, b int) int {
		if a >= b {
			return a
		}
		return b
	}

	whiteSpace := strings.Repeat(" ", termWidth)
	writeCmdLine := func() {
		write(0, termHeight-2, whiteSpace, false)
		write(0, termHeight-1, whiteSpace, false)
		write(0, termHeight-1, "> ", false)
		write(0, termHeight-2, fmt.Sprintf("%d:%d (%d)", commandStartIndex, commandLength, commandCursorIndex), false)
		min(0, max(0, 1))
		if commandCursorIndex-commandStartIndex > termWidth-4 {
			commandStartIndex = commandCursorIndex - (termWidth - 4)
		} else if commandCursorIndex <= commandStartIndex {
			commandStartIndex = max(commandCursorIndex-1, 0)
		}

		if commandStartIndex > 0 {
			write(2, termHeight-1, command[commandStartIndex:], false)
		} else {
			write(2, termHeight-1, command, false)
		}
		termbox.SetCursor(2+commandCursorIndex-commandStartIndex, termHeight-1)
		termbox.Flush()
	}

	for isRunning {
		clear()
		currentUI.PrintUI()
		termbox.Flush()

		for isRunning {
			event := termbox.PollEvent()
			if event.Type == termbox.EventKey {
				isRunning = event.Key != termbox.KeyCtrlQ

				if event.Key == termbox.KeyEsc {
					if currentUI == rootUI {
						isRunning = false
						break
					}
					currentUI = currentUI.ParentState
					break
				} else if event.Key == termbox.KeyEnter {
					if !tryParseAction(command) {
						if currentUI.HandleInput != nil {
							currentUI.HandleInput(command)
						} else {
							clear()
							write(4, 4, "unknown command: "+command, true)
							termbox.Flush()
						}
					}
					command = ""
					commandLength = 0
					commandStartIndex = 0
					commandCursorIndex = 0
				} else if event.Key == termbox.KeyBackspace {
					if commandCursorIndex > 0 {
						if commandCursorIndex == commandLength {
							command = command[:commandLength-1]
						} else {
							command = command[:commandCursorIndex-1] + command[commandCursorIndex:] //command[:len(command)-1]
						}
						commandLength--
						commandCursorIndex--
					}
					writeCmdLine()
				} else if event.Key == termbox.KeyArrowLeft {
					if commandCursorIndex > 0 {
						commandCursorIndex--
					}
					writeCmdLine()
				} else if event.Key == termbox.KeyArrowRight {
					if commandCursorIndex < commandLength {
						commandCursorIndex++
					}
					writeCmdLine()
				} else {
					if commandCursorIndex == commandLength {
						command = command + string(event.Ch)
					} else if commandCursorIndex == 0 {
						command = string(event.Ch) + command
					} else {
						command = command[:commandCursorIndex] + string(event.Ch) + command[commandCursorIndex:]
					}
					commandLength++
					commandCursorIndex++
					writeCmdLine()
				}
			}
		}
	}
}
