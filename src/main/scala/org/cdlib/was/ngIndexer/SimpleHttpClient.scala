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

  /**
   * Send an HTTP request, and process the response.
   *
   * @param request The HTTP request.
   * @param f A function which takes a pair (Int, HttpResponse)
   *   and returns a T. Generally will be a set of case statements.
   * @return The value returned by f.
   */
  def mkRequest[T] (request : HttpUriRequest) 
                   (f : (Pair[Int, HttpResponse]) => T) : T = {
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
    
  /**
   * Get a redirect URI for an HttpResponse.
   *
   * @return The URI found, or None.
   */
  private def getRedir (resp : HttpResponse) : Option[URI] = {
    val header = resp.getFirstHeader("Location");
    if (header == null) {
      return None;
    } else {
      val newUri = new URI(header.getValue);
      return Some(newUri);
    }
  }

  /**
   * GET a URI, following redirects. Call the given function on the body
   * if we get a 200 response.
   */
  def getUri[T] (uri : URI) (f : (InputStream)=>T) : Option[T] = {
    return mkRequest(new HttpGet(uri)) {
      case (200, resp) => Some(f(resp.getEntity.getContent));
      case ((301 | 302 | 303), resp) => getRedir(resp).flatMap(newUri=>getUri(newUri)(f));
      case (_,   _)    => None;
    }
  }
  
  /**
   * Get a URI, returning the response. Follows redirects. Return
   * None if we get a non-200 response.
   *
   * Remember to call org.apache.http.util.EntityUtils.consume(response.getEntity) when
   * finished.
   */
  def getUriResponse (uri : URI) : Option[HttpResponse] = {
    var response : HttpResponse = null
    var statusCode : Int = -1;
    try {
      response = httpClient.execute(new HttpGet(uri));
      statusCode = response.getStatusLine.getStatusCode;
      statusCode match {
        case 200               => Some(response);
        case (301 | 302 | 303) => getRedir(response).flatMap(newUri=>getUriResponse(newUri));
        case _                 => None;
      }
    } finally {
      if ((response != null) && (response.getEntity != null) &&
          (statusCode != 200)) {
            /* If we did not pass the content stream back to the user,
             consume it. */
            EntityUtils.consume(response.getEntity);
          }
    }
  }
}
