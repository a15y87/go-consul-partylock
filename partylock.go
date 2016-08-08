package consul_partylock

import "time"

type PartyLock struct {
	ConsulClient *consulLockClient
	LockTimeout  time.Duration
	Capacity     int
}

func New(TaskPath string, TaskWeight string, ConsulAddress string, LockTimeout time.Duration, Capacity int) (partyLock *PartyLock, err error) {

	var LockClient consulLockClient

	err = LockClient.Init(TaskPath, TaskWeight, ConsulAddress)
	if err != nil {
		return nil, err
	}
	partyLock = &PartyLock{&LockClient, LockTimeout, Capacity}

	return partyLock, err
}

func (s *PartyLock) Lock() (status bool, err error) {
	err = s.ConsulClient.AddWait()
	if err != nil {
		return false, err
	}

	time.Sleep(s.LockTimeout)
	myPosition, err := s.ConsulClient.GetWaitPosition()
	if err != nil {
		return false, s.ConsulClient.DeleteWait()
	}
	if myPosition > (s.Capacity - 1) {
		return false, s.ConsulClient.DeleteWait()
	}

	locksCount, err := s.ConsulClient.GetLocksCount()
	if err != nil {
		return false, s.ConsulClient.DeleteWait()
	}

	err = s.ConsulClient.DeleteWait()
	if err != nil {
		return false, err
	}

	if locksCount > (s.Capacity - 1) {
		return false, err
	}

	status, err = s.ConsulClient.AddLock()
	return status, err
}

func (s *PartyLock) UnLock() (status bool, err error) {
	time.Sleep(s.LockTimeout)
	return s.ConsulClient.DeleteLock()
}

func (s *PartyLock) UpdateLock() (status bool, err error) {
	return s.ConsulClient.AddLock()
}