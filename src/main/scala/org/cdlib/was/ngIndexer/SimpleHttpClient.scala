package org.cdlib.was.ngIndexer;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.{HttpGet,HttpUriRequest}
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.scheme.{PlainSocketFactory,Scheme,SchemeRegistry}
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.{BasicHttpParams,HttpConnectionParams,HttpProtocolParams};
import org.apache.http.{HttpException,HttpResponse,HttpVersion};

import java.io.InputStream;

import java.net.URI;

class SimpleHttpClient {
  val schemeRegistry = new SchemeRegistry();
  schemeRegistry.register(
    new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
  /* setup params */
  val params = new BasicHttpParams();
  ConnManagerParams.setMaxTotalConnections(params, 100);
  HttpConnectionParams.setConnectionTimeout(params, 20 * 1000);
  HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
  val cm = new ThreadSafeClientConnManager(params, schemeRegistry);
  var httpClient = new DefaultHttpClient(cm, params);

  def mkRequest[T] (request : HttpUriRequest) (f : (Pair[Int, HttpResponse]) => T) : T = {
    var response : HttpResponse = null;
    try {
      response = httpClient.execute(request);
      return f ((response.getStatusLine.getStatusCode, response));
    } finally {
      if ((response != null) && (response.getEntity != null))
        { response.getEntity.consumeContent; }
    }      
  }

  def mkRequestExcept[T](req : HttpGet) (f : (HttpResponse)=>T) : T = {
    mkRequest(req) {
      case (200, resp) => f (resp);
      case (_,   resp) => throw new HttpException(resp.getStatusLine.toString);
    }
  }

  def getUri[T] (uri : URI) (f : (InputStream)=>T) = {
    mkRequestExcept(new HttpGet(uri)) {(resp)=>
      f(resp.getEntity.getContent);
    }
  }
}
