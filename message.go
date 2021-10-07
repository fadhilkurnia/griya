package main

type PrepareRequest struct {
	BallotNumber uint64 `json:"ballot_number"`
}

// PrepareResponse or Promise
type PrepareResponse struct {
	IsSuccess           bool   `json:"is_success"`
	HighestBallotNumber uint64 `json:"highest_ballot_number"`
	LastAcceptedValue   []byte `json:"last_accepted_value"`
}

type ProposeRequest struct {
	BallotNumber uint64 `json:"ballot_number"`
	Value        []byte `json:"value"`
}

type ProposeResponse struct {
	IsAccepted bool `json:"is_accepted"`
}
