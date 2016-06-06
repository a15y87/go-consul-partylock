package consul_partylock

import (
	"github.com/hashicorp/consul/api"
	"sort"
	"strings"
)

type ConsulLockClient struct {
	TaskPath      string
	TaskWeight    string
	ConsulAddress string

	consulSession      string
	consulClient       *api.Client
	consulPathWait     string
	consulPathWaitTask string
	consulPathLock     string
	consulPathLockTask string
}

func (c *ConsulLockClient) Init(TaskPath, TaskWeight, ConsulAddress string) (err error) {
	se := &api.SessionEntry{}
	se.TTL = "10s"
	se.Behavior = "delete"

	ConsulConfig := api.DefaultConfig()
	ConsulConfig.Address = c.ConsulAddress

	c.consulClient, err = api.NewClient(ConsulConfig)
	if err != nil {
		return err
	}

	c.consulSession, _, err = c.consulClient.Session().Create(se, nil)
	if err != nil {
		return err
	}

	c.consulPathWait = strings.Join([]string{c.TaskPath, "/.wait/"}, "")
	c.consulPathLock = strings.Join([]string{c.TaskPath, "/.locked/"}, "")
	c.consulPathWaitTask = strings.Join([]string{c.consulPathWaitTask, c.TaskWeight, "-", c.consulSession}, "")
	c.consulPathLockTask = strings.Join([]string{c.consulPathLock, c.TaskWeight, "-", c.consulSession}, "")

	return nil
}

func (c *ConsulLockClient) AddWait() (err error) {
	kv := c.consulClient.KV()
	p := &api.KVPair{Key: c.consulPathWaitTask, Value: []byte("0"), Session: c.consulSession}
	_, _, err = kv.Acquire(p, nil)
	return err
}

func (c *ConsulLockClient) DeleteWait() (err error) {
	kv := c.consulClient.KV()
	_, err = kv.Delete(c.consulPathWaitTask, nil)
	return err
}

func (c *ConsulLockClient) GetWaitPosition() (position int, err error) {
	kv := c.consulClient.KV()
	waits, _, err := kv.Keys(c.consulPathWait, ";", nil)
	if err != nil {
		return 0, err
	}
	position = sort.SearchStrings(waits, c.consulPathWaitTask)
	return position, err
}

func (c *ConsulLockClient) AddLock() (status bool, err error) {
	kv := c.consulClient.KV()
	p := &api.KVPair{Key: c.consulPathLockTask, Value: []byte("0"), Session: c.consulSession}
	status, _, err = kv.Acquire(p, nil)
	return status, err
}

func (c *ConsulLockClient) DeleteLock() (status bool, err error) {
	kv := c.consulClient.KV()
	return kv.Delete(c.consulPathLockTask, nil)

}

func (c *ConsulLockClient) GetLocksCount() (count int, err error) {
	kv := c.consulClient.KV()
	locks, _, err := kv.Keys(c.consulPathLock, ";", nil)
	if err != nil {
		return 0, err
	}
	count = len(locks)
	return count, err
}

func (c *ConsulLockClient) DestroySession() (status bool, err error) {
	return c.consulClient.Session().Destroy(c.consulSession, nil)
}
