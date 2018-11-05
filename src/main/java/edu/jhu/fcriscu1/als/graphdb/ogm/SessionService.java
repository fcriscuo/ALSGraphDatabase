package edu.jhu.fcriscu1.als.graphdb.ogm;

import java.util.function.Supplier;


import org.neo4j.ogm.config.Configuration;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;

public enum SessionService implements Supplier<Session> {
  INSTANCE;
  private static Configuration configuration = new Configuration.Builder()
      .uri("bolt://localhost")
      .credentials("neo4j", "fred3372")
      .build();
  private static SessionFactory sessionFactory = new SessionFactory(configuration,
      "edu.jhu.fcriscu1.als.graphdb.ogm");


  @Override
  public Session get() {
    return sessionFactory.openSession();
  }

  public static void main(String[] args) {
    try {
      Session session = SessionService.INSTANCE.get() ;

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
