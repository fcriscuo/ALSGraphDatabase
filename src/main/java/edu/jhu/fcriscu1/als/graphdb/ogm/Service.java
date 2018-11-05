package edu.jhu.fcriscu1.als.graphdb.ogm;

interface Service<T> {

  Iterable<T> findAll();

  T find(Long id);

  void delete(Long id);

  T createOrUpdate(T object);

}