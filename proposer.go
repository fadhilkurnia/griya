package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

type ProposerState struct {
	CurrentBallotNumber uint64
	CommittedValue      []byte
	Lock                sync.Mutex
}

// ProposerServer receive request from client
// then coordinate it with acceptors
func RunProposerServer(proposerID uint64, isOblivious bool) {
	var state = ProposerState{}

	proposerPort := 9900 + proposerID
	proposerHost := fmt.Sprintf(":%d", proposerPort)
	proposerServer := &http.Server{
		Addr:         proposerHost,
		Handler:      initProposerHandler(&state),
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
	}

	fmt.Println("griya:: starting proposer", proposerID, "on", proposerHost)
	err := proposerServer.ListenAndServe()
	if err != nil {
		panic(err)
	}
}

func initProposerHandler(proposerState *ProposerState) http.Handler {
	gin.SetMode(gin.ReleaseMode)
	e := gin.New()
	e.Use(gin.Recovery())
	e.Use(gin.Logger())

	// main endpoint to receive client request data
	e.POST("/", func(c *gin.Context) {
		clientResponse := gin.H{
			"code":  http.StatusBadRequest,
			"error": "failed to process the request",
		}

		data, err := ioutil.ReadAll(c.Request.Body)
		if err != nil {
			fmt.Println("failed to read client request", err)
			c.JSON(http.StatusBadRequest, clientResponse)
			return
		}

		// coordinate data with all acceptors
		var nAcceptors = uint64(3)
		var majority = (nAcceptors + 1) / 2
		var replyLock = sync.Mutex{}

		// ======= BEGIN PREPARE PHASE ========================================
		prepareRequest := PrepareRequest{}

		// TODO: make the ballot number unique for each proposer
		// increase the ballot number of this proposer
		proposerState.Lock.Lock()
		proposerState.CurrentBallotNumber++
		prepareRequest.BallotNumber = proposerState.CurrentBallotNumber
		proposerState.Lock.Unlock()
		var lastAcceptedValue = []byte{}
		var highestBallotNumber = prepareRequest.BallotNumber
		var replyCount = uint64(0) // TODO: change this with channel

		requestBody, _ := json.Marshal(prepareRequest)

		for i := uint64(0); i < nAcceptors; i++ {
			go func(acceptorID uint64) {
				acceptorPort := 9800 + acceptorID
				acceptorEndpointStr := fmt.Sprintf("http://127.0.0.1:%d/prepare", acceptorPort)
				response, err := http.Post(acceptorEndpointStr, "application/json", bytes.NewReader(requestBody))
				if err != nil {
					fmt.Println("ERROR! failed to send prepare request to acceptor", acceptorPort, err)
					return
				}
				// check if we get success response
				responseByte, err := ioutil.ReadAll(response.Body)
				if err != nil {
					fmt.Println("FATAL! failed to read prepare response data from acceptor", err)
					return
				}
				var prepareResponse = PrepareResponse{}
				err = json.Unmarshal(responseByte, &prepareResponse)
				if err != nil {
					fmt.Println("FATAL! failed to parse prepare response data from acceptor", err)
					return
				}

				fmt.Println("[", acceptorID, "] Prepare response: ", prepareResponse)

				replyLock.Lock()

				replyCount++

				// Some acceptors already accepted a values, this proposer
				// need to propose that accapted value in the next phase.
				// Value from the highest ballot is the one that this proposer
				// will propose next.
				if len(prepareResponse.LastAcceptedValue) > 0 &&
					prepareResponse.HighestBallotNumber >= highestBallotNumber {
					lastAcceptedValue = prepareResponse.LastAcceptedValue
				}

				// There is other proposer that has higher ballot number
				// this proposer need to back down
				if prepareResponse.HighestBallotNumber > highestBallotNumber {
					highestBallotNumber = prepareResponse.HighestBallotNumber
				}

				replyLock.Unlock()
			}(i)
		}

		// TODO: handle if the proposer does not get response from the majority
		// (timeout), then return error to client since we can not do the
		// consensus protocol

		// wait until we get majority response
		for {
			if replyCount >= majority {
				break
			}
		}
		fmt.Println("finish phase 1", highestBallotNumber, prepareRequest)

		// ======= END PREPARE PHASE ==========================================

		// There is other proposer with higher ballot number, this proposer
		// need to back down. This proposer later can try again with higher
		// unique ballot number.
		if highestBallotNumber > prepareRequest.BallotNumber {

			// Increasing the ballot number for this proposer
			// TODO: make sure the ballot number is unique for this proposer
			proposerState.Lock.Lock()
			proposerState.CurrentBallotNumber = highestBallotNumber + 1
			proposerState.Lock.Unlock()

			clientResponse["error"] = "please try again later"
			clientResponse["code"] = http.StatusConflict

			c.JSON(http.StatusConflict, clientResponse)
			return
		}

		// If some acceptors send an already accepted value, this proposer
		// need to propose that value, not the value given by the client.
		isClientDataAccepted := true
		if len(lastAcceptedValue) > 0 {
			isClientDataAccepted = false

			// TODO: we can directly reply to client here, while the proposer
			// still need to propose the already accepted value.
		}

		// ======= BEGIN PROPOSE PHASE ========================================

		proposeRequest := ProposeRequest{}
		proposeRequest.BallotNumber = prepareRequest.BallotNumber
		if isClientDataAccepted {
			proposeRequest.Value = data
		} else {
			proposeRequest.Value = lastAcceptedValue
		}

		proposePayload, _ := json.Marshal(proposeRequest)
		proposeReplyCount := uint64(0)
		proposeAcceptCount := uint64(0)
		proposeReplyLock := sync.Mutex{}

		for i := uint64(0); i < nAcceptors; i++ {
			go func(acceptorID uint64) {
				acceptorPort := 9800 + acceptorID
				acceptorEndpointStr := fmt.Sprintf("http://127.0.0.1:%d/propose", acceptorPort)
				response, err := http.Post(acceptorEndpointStr, "application/json", bytes.NewReader(proposePayload))
				if err != nil {
					fmt.Println("ERROR! failed to send propose request to acceptor", acceptorPort, err)
					return
				}
				// check if we get success response
				responseByte, err := ioutil.ReadAll(response.Body)
				if err != nil {
					fmt.Println("FATAL! failed to read propose response data from acceptor", err)
					return
				}
				var proposeResponse = ProposeResponse{}
				err = json.Unmarshal(responseByte, &proposeResponse)
				if err != nil {
					fmt.Println("FATAL! failed to parse propose response data from acceptor", err)
					return
				}

				proposeReplyLock.Lock()
				proposeReplyCount++
				if proposeResponse.IsAccepted {
					proposeAcceptCount++
				}
				proposeReplyLock.Unlock()

			}(i)
		}

		// TODO: need to handle timeout (acceptor does not send anything)

		for {
			if proposeReplyCount >= majority || proposeAcceptCount >= majority {
				break
			}
		}

		// this proposer already get majority response, but not majority accept
		if proposeAcceptCount < majority {
			isClientDataAccepted = false
		}

		// ======= END PROPOSE PHASE ========================================

		if isClientDataAccepted {
			// TODO: store accepted value in a persistent storage

		} else {
			c.JSON(
				http.StatusConflict,
				gin.H{
					"code":  http.StatusConflict,
					"error": "please try again",
				},
			)
			return
		}

		c.JSON(
			http.StatusOK,
			gin.H{
				"code":    http.StatusOK,
				"message": "your data is replicated successfully",
			},
		)
	})

	return e
}
