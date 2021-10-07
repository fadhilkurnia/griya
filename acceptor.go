package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

type AcceptorState struct {
	ID                    uint64
	HighestBallotNumber   uint64
	AcceptedValue         []byte
	CommittedBallotNumber uint64
	CommittedValue        []byte
	Lock                  sync.Mutex
}

func RunAcceptorServer(acceptorID uint64) {
	var state = AcceptorState{ID: acceptorID}
	acceptorPort := 9800 + acceptorID
	acceptorHost := fmt.Sprintf(":%d", acceptorPort)

	acceptorServer := &http.Server{
		Addr:         acceptorHost,
		Handler:      initAcceptorHandler(&state),
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
	}

	fmt.Println("griya:: starting acceptor", acceptorID, "on", acceptorHost)
	err := acceptorServer.ListenAndServe()
	if err != nil {
		panic(err)
	}
}

func initAcceptorHandler(acceptorState *AcceptorState) http.Handler {
	gin.SetMode(gin.ReleaseMode)
	e := gin.New()
	e.Use(gin.Recovery())
	e.Use(gin.Logger())

	e.POST("/prepare", handlePrepareRequest(acceptorState))
	e.POST("/propose", handleProposeRequest(acceptorState))

	return e
}

func handlePrepareRequest(acceptorState *AcceptorState) gin.HandlerFunc {
	fn := func(c *gin.Context) {
		prepareResponse := PrepareResponse{
			IsSuccess:           false,
			HighestBallotNumber: 0,
			LastAcceptedValue:   nil,
		}

		payload, err := ioutil.ReadAll(c.Request.Body)
		if err != nil {
			fmt.Println("failed to read prepare request data", err)
			c.JSON(http.StatusBadRequest, prepareResponse)
			return
		}
		prepareRequest := PrepareRequest{}
		err = json.Unmarshal(payload, &prepareRequest)
		if err != nil {
			fmt.Println("failed to parse prepare request data", err)
			c.JSON(http.StatusBadRequest, prepareResponse)
			return
		}

		acceptorState.Lock.Lock()

		// the ballot number given is too small, need higher number
		// to make this acceptor return promise not to accept other proposal
		if prepareRequest.BallotNumber <= acceptorState.HighestBallotNumber {
			acceptorState.Lock.Unlock()
			fmt.Println("ballot number is too small")
			c.JSON(http.StatusOK, prepareResponse)
			return
		}

		// save the new highest ballot number
		acceptorState.HighestBallotNumber = prepareRequest.BallotNumber
		prepareResponse.HighestBallotNumber = acceptorState.HighestBallotNumber

		// send the previously accepted value, if any
		if len(acceptorState.AcceptedValue) > 0 {
			prepareResponse.LastAcceptedValue = acceptorState.AcceptedValue
		}

		acceptorState.Lock.Unlock()

		c.JSON(
			http.StatusOK,
			prepareResponse,
		)
	}

	return gin.HandlerFunc(fn)
}

func handleProposeRequest(acceptorState *AcceptorState) gin.HandlerFunc {
	fn := func(c *gin.Context) {
		proposeResponse := ProposeResponse{
			IsAccepted: false,
		}

		payload, err := ioutil.ReadAll(c.Request.Body)
		if err != nil {
			fmt.Println("failed to read propose request data", err)
			c.JSON(http.StatusBadRequest, proposeResponse)
			return
		}
		proposeRequest := ProposeRequest{}
		err = json.Unmarshal(payload, &proposeRequest)
		if err != nil {
			fmt.Println("failed to parse propose request data", err)
			c.JSON(http.StatusBadRequest, proposeResponse)
			return
		}

		acceptorState.Lock.Lock()
		if acceptorState.HighestBallotNumber != proposeRequest.BallotNumber {
			acceptorState.Lock.Unlock()
			fmt.Println("can not commit unpromised proposal", err)
			c.JSON(http.StatusBadRequest, proposeResponse)
			return
		}

		// the ballot number is equal with the highest ballot number that this
		// acceptor has seen (and has promised to accept its proposal)
		// TODO: store comitted value in a persistent storage
		acceptorState.CommittedValue = proposeRequest.Value
		acceptorState.CommittedBallotNumber = proposeRequest.BallotNumber
		proposeResponse.IsAccepted = true

		acceptorState.Lock.Unlock()

		c.JSON(
			http.StatusOK,
			proposeResponse,
		)
	}

	return gin.HandlerFunc(fn)
}
