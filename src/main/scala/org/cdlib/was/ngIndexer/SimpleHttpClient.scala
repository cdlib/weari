package org.cdlib.was.ngIndexer;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.{HttpGet,HttpUriRequest}
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.scheme.{PlainSocketFactory,Scheme,SchemeRegistry}
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.{BasicHttpParams,HttpConnectionParams,HttpProtocolParams};
import org.apache.http.{HttpException,HttpResponse,HttpVersion};
import org.apache.http.util.EntityUtils;

import java.io.InputStream;

import java.net.URI;

class SimpleHttpClient {
  val schemeRegistry = new SchemeRegistry();
  schemeRegistry.register(
    new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
  /* setup params */
  val params = new BasicHttpParams();
  HttpConnectionParams.setConnectionTimeout(params, 20 * 1000);
  HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
  val cm = new ThreadSafeClientConnManager(schemeRegistry);
  cm.setDefaultMaxPerRoute(100);
  cm.setMaxTotal(100);
  var httpClient = new DefaultHttpClient(cm, params);

  def mkRequest[T] (request : HttpUriRequest) (f : (Pair[Int, HttpResponse]) => T) : T = {
    var response : HttpResponse = null;
    try {
      response = httpClient.execute(request);
      return f ((response.getStatusLine.getStatusCode, response));
    } finally {
      if ((response != null) && (response.getEntity != null))
        { EntityUtils.consume(response.getEntity); }
    }      
  }

  def mkRequestExcept[T](req : HttpGet) (f : (HttpResponse)=>T) : T = {
    mkRequest(req) {
      case (200, resp) => f (resp);
      case (_,   resp) => throw new HttpException(resp.getStatusLine.toString);
    }
  }

  def getUri[T] (uri : URI) (f : (InputStream)=>T) : Option[T] = {
    def followRedir (resp : HttpResponse) : Option[T] = {
      val header = resp.getFirstHeader("Location");
      if (header == null) {
        return None;
      } else {
        val newUri = new URI(header.getValue);
        return getUri[T](newUri)(f);
      }
    }
    return mkRequest(new HttpGet(uri)) {
      case (200, resp) => Some(f(resp.getEntity.getContent));
      case (301, resp) => followRedir(resp);
      case (302, resp) => followRedir(resp);
      case (303, resp) => followRedir(resp);
      case (_,   _)    => None;
    }
  }
}
