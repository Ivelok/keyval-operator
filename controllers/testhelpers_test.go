package controllers

type assertError string

func (e assertError) Error() string { return string(e) }
