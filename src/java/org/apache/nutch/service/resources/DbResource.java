/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.service.resources;


import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.CrawlDbReader;
import org.apache.nutch.fetcher.FetchNode;
import org.apache.nutch.fetcher.FetchNodeDb;
import org.apache.nutch.service.model.request.DbQuery;
import org.apache.nutch.service.model.response.FetchNodeDbInfo;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Path(value = "/db")
public class DbResource extends AbstractResource {

  @POST
  @Path(value = "/crawldb")
  @Consumes(MediaType.APPLICATION_JSON)
  public Object readdb(DbQuery dbQuery){
    Configuration conf = configManager.get(dbQuery.getConfId());
    String type = dbQuery.getType();

    if(type.equalsIgnoreCase("stats")){
      return crawlDbStats(conf, dbQuery.getArgs(), dbQuery.getCrawlId());
    }
    if(type.equalsIgnoreCase("dump")){
      return crawlDbDump(conf, dbQuery.getArgs(), dbQuery.getCrawlId());
    }
    if(type.equalsIgnoreCase("topN")){
      return crawlDbTopN(conf, dbQuery.getArgs(), dbQuery.getCrawlId());
    }
    if(type.equalsIgnoreCase("url")){
      return crawlDbUrl(conf, dbQuery.getArgs(), dbQuery.getCrawlId());
    }
    return null;

  }	
  
  @GET
  @Path(value="/fetchdb")
  public List<FetchNodeDbInfo> fetchDb(@DefaultValue("0")@QueryParam("to")int to, @DefaultValue("0")@QueryParam("from")int from){
    List<FetchNodeDbInfo> listOfFetchedNodes = new ArrayList<FetchNodeDbInfo>();
    Map<Integer, FetchNode> fetchNodedbMap = FetchNodeDb.getInstance().getFetchNodeDb();
    
    if(to ==0 || to>fetchNodedbMap.size()){
      to = fetchNodedbMap.size();
    }
    for(int i=from;i<=to;i++){
      if(!fetchNodedbMap.containsKey(i)){
        continue;
      }
      FetchNode node = fetchNodedbMap.get(i);
      FetchNodeDbInfo fdbInfo = new FetchNodeDbInfo();
      fdbInfo.setUrl(node.getUrl().toString());
      fdbInfo.setStatus(node.getStatus());
      fdbInfo.setNumOfOutlinks(node.getOutlinks().length);
      fdbInfo.setChildNodes(node.getOutlinks());
      listOfFetchedNodes.add(fdbInfo);
    }
    
    return listOfFetchedNodes;
  }
  @SuppressWarnings("resource")
  private Response crawlDbStats(Configuration conf, Map<String, String> args, String crawlId){
    CrawlDbReader dbr = new CrawlDbReader();
    try{
      return Response.ok(dbr.query(args, conf, "stats", crawlId)).build();
    }catch(Exception e){
      e.printStackTrace();
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  private Response crawlDbDump(Configuration conf, Map<String, String> args, String crawlId){
    CrawlDbReader dbr = new CrawlDbReader();
    try{
      return Response.ok(dbr.query(args, conf, "dump", crawlId), MediaType.APPLICATION_OCTET_STREAM).build();
    }catch(Exception e){
      e.printStackTrace();
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  private Response crawlDbTopN(Configuration conf, Map<String, String> args, String crawlId) {
    CrawlDbReader dbr = new CrawlDbReader();
    try{
      return Response.ok(dbr.query(args, conf, "topN", crawlId), MediaType.APPLICATION_OCTET_STREAM).build();
    }catch(Exception e){
      e.printStackTrace();
      return Response.serverError().entity(e.getMessage()).build();
    }		
  }

  private Response crawlDbUrl(Configuration conf, Map<String, String> args, String crawlId){
    CrawlDbReader dbr = new CrawlDbReader();
    try{
      return Response.ok(dbr.query(args, conf, "url", crawlId)).build();
    }catch(Exception e){
      e.printStackTrace();
      return Response.serverError().entity(e.getMessage()).build();
    }
  }
}
