Radix
=====

Package radix implement a
[radix tree](http://en.wikipedia.org/wiki/Radix_tree) in go.  It is
expected that the keys are in UTF-8 (i.e. go runes), and that
insertion and lookup is far more common than deletion.


Quick usage
-----------

    go get github.com/js-arias/radix

The main data structure is Radix, that is initialized with New, e.g.

    import "github.com/js-arias/radix"
    var r = radix.New()

The implementation of radix is private. To access data use:

    func (r *Radix) Insert(key string, value interface{}) error
    
Insert a value in the radix. It returns an error if the key is already
in use.

    func (r *Radix) Lookup(key string) interface{}
    
Searches for a particular key.

    func (r *Radix) Prefix(key string) *list.List

Return all elements that has key as its prefix.

    func (r *Radix) Delete(key string) interface{}
    
Removes an element from the radix, and returns it.

The radix can be navigated using an iterator that scan the radix
in alphabetical order:

    for it := r.Iterator(); it != nil; it.Next() {
        // do something with it.Value or it.Key
    }

Authorship and license
----------------------

Copyright (c) 2013, J. Salvador Arias <jsalarias@csnat.unt.edu.ar>
All rights reserved.
Distributed under BSD2 license that can be found in the LICENSE file.

