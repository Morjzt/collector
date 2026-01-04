package main

import (
	"crypto/sha256"
	"fmt"
	"time"
)

type JobContext struct {
	ID			string    `json:"id"`
	Source      string    `json:"source"`
	PayloadHash string    `json:"payload_hash"` 
	RawData     string    `json:"raw_data"`    
	Timestamp   time.Time `json:"timestamp"`
}