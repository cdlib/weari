package org.cdlib.was.weari.tests;

import com.gu.integration._;
import scalax.file.Path;
import org.scalatest._;

object ContentApi extends AppServer with LazyStop {
  override val port = 8700
  lazy val apps = List(SolrWebApp)

  object SolrWebApp extends WarWebApp {
    lazy val warPath = "solr-server/src/main/solr-server/webapps/solr.war"
    override lazy val contextPath = "/solr"

    override def preStart() {
      System.setProperty("solr.solr.home",
                         SiblingProjectFile("solr-server/src/main/solr-server/solr").path)
      System.setProperty("solr.data.dir", Path("integration/target/dependency/solr/data").toAbsolute.path);
      System.setProperty("master.enable", "true");
    }
  }
}

trait RequiresRunningContentApi extends Suite with BeforeAndAfterAll  {
  override protected def beforeAll() { ContentApi.start() }
  override protected def afterAll() { ContentApi.stopUnlessSomeoneCallsStartAgainSoon() }
}
