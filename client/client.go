package main

import (
	"errors"
	"fmt"
	"net/rpc"
)

const (
	ONE      = "ONE"
	QUORUM   = "QUORUM"
	ALL      = "ALL"
	GetOp    = "SwimRing.Get"
	PutOp    = "SwimRing.Put"
	DeleteOp = "SwimRing.Delete"
)

type SwimringClient struct {
	address string
	port    int
	client  *rpc.Client

	readLevel  string
	writeLevel string
}

type GetRequest struct {
	Level string
	Key   string
}

type GetResponse struct {
	Key, Value string
}

type PutRequest struct {
	Level      string
	Key, Value string
}

type PutResponse struct{}

type DeleteRequest struct {
	Level string
	Key   string
}

type DeleteResponse struct{}

func NewSwimringClient(address string, port int) *SwimringClient {
	c := &SwimringClient{
		address:    address,
		port:       port,
		readLevel:  ALL,
		writeLevel: ALL,
	}

	return c
}

func (c *SwimringClient) SetReadLevel(level string) {
	c.readLevel = level
}

func (c *SwimringClient) SetWriteLevel(level string) {
	c.writeLevel = level
}

func (c *SwimringClient) Connect() error {
	var err error
	c.client, err = rpc.Dial("tcp", fmt.Sprintf("%s:%d", c.address, c.port))
	if err != nil {
		return err
	}

	return nil
}

func (c *SwimringClient) Get(key string) (string, error) {
	if c.client == nil {
		return "", errors.New("not connected")
	}

	req := &GetRequest{
		Key:   key,
		Level: c.readLevel,
	}
	resp := &GetResponse{}

	err := c.client.Call(GetOp, req, resp)
	if err != nil {
		return "", err
	}

	return resp.Value, nil
}

func (c *SwimringClient) Put(key, value string) error {
	if c.client == nil {
		return errors.New("not connected")
	}

	req := &PutRequest{
		Key:   key,
		Value: value,
		Level: c.writeLevel,
	}
	resp := &PutResponse{}

	err := c.client.Call(PutOp, req, resp)
	if err != nil {
		return err
	}

	return nil
}

func (c *SwimringClient) Delete(key string) error {
	if c.client == nil {
		return errors.New("not connected")
	}

	req := &DeleteRequest{
		Key:   key,
		Level: c.writeLevel,
	}
	resp := &DeleteResponse{}

	err := c.client.Call(DeleteOp, req, resp)
	if err != nil {
		return err
	}

	return nil
}
