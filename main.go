package main

import (
	"flag"
	"fmt"
	"runtime"
	"strings"
)

// handle args given by user
func parseArgs() (uint64, []string, bool, error) {
	var (
		nodeID    = flag.Int("id", 0, "Griya node id (default 0)")
		roles     = flag.String("roles", "acceptor", "Role(s) of the node, separate them with comma.\nValid roles: acceptor,proposer.\nExample: acceptor,proposer.\n")
		oblivious = flag.Bool("oblivious", false, "Using threshold secret sharing to distribute data")
	)

	flag.Parse()
	if *nodeID < 0 {
		return 0, nil, false, fmt.Errorf(
			"node id should be greater or equal than zero (%d)",
			*nodeID,
		)
	}
	rolesArr := strings.Split(*roles, ",")
	if len(rolesArr) == 0 {
		return 0, nil, false, fmt.Errorf(
			"wrong role given, valid roles are 'acceptor' and 'proposer'",
		)
	}
	for i := 0; i < len(rolesArr); i++ {
		if rolesArr[i] != "acceptor" && rolesArr[i] != "proposer" {
			return 0, nil, false, fmt.Errorf(
				"wrong role given (%s), valid roles are 'acceptor' and 'proposer'",
				rolesArr[i],
			)
		}
	}

	return uint64(*nodeID), rolesArr, *oblivious, nil
}

func main() {
	fmt.Println(":: privacy preserving smart home prototype (griya) ::")

	// parsing the args given by user
	nodeID, roles, isOblivious, err := parseArgs()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	for _, role := range roles {
		switch {
		case role == "proposer":
			go RunProposerServer(nodeID, isOblivious)
		case role == "acceptor":
			go RunAcceptorServer(nodeID)
		}
	}

	// does not exit the program until all the goroutines are finished
	runtime.Goexit()
}
