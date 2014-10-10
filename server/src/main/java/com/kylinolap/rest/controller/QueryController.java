/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the <dependency>
    <groupId>com.ryantenney.metrics</groupId>
    <artifactId>metrics-spring</artifactId>
    <version>3.0.0</version>
</dependency>che License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.rest.controller;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletResponse;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

import com.codahale.metrics.annotation.Timed;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.rest.constant.Constant;
import com.kylinolap.rest.exception.InternalErrorException;
import com.kylinolap.rest.model.SelectedColumnMeta;
import com.kylinolap.rest.model.TableMeta;
import com.kylinolap.rest.request.MetaRequest;
import com.kylinolap.rest.request.PrepareSqlRequest;
import com.kylinolap.rest.request.SQLRequest;
import com.kylinolap.rest.request.SaveSqlRequest;
import com.kylinolap.rest.response.GeneralResponse;
import com.kylinolap.rest.response.SQLResponse;
import com.kylinolap.rest.service.QueryService;

/**
 * Handle query requests.
 * 
 * @author xduo
 */
@Controller
public class QueryController extends BasicController {

	private static final Logger logger = LoggerFactory
			.getLogger(QueryController.class);

	public static final String SUCCESS_QUERY_CACHE = "SuccessQueryCache";
	public static final String EXCEPTION_QUERY_CACHE = "ExceptionQueryCache";

	@Autowired
	private QueryService queryService;

	@Autowired
	private CacheManager cacheManager;

	// refactor SQLResponse, drop ODBC stuff, duration type long
	@RequestMapping(value = "/query", method = RequestMethod.POST)
	@ResponseBody
	@Timed(name = "query")
	public SQLResponse query(@RequestBody SQLRequest sqlRequest) {
		long startTimestamp = System.currentTimeMillis();

		SQLResponse response = doQuery(sqlRequest);
		response.setDuration(System.currentTimeMillis() - startTimestamp);

		queryService.logQuery(sqlRequest, response, new Date(startTimestamp),
				new Date(System.currentTimeMillis()));

		if (response.getIsException()) {
			String errorMsg = response.getExceptionMessage();
			throw new InternalErrorException(makeErrorMsgUserFriendly(errorMsg));
		}

		return response;
	}

	@RequestMapping(value = "/query/prestate", method = RequestMethod.POST, produces = "application/json")
	@ResponseBody
	@Timed(name = "query")
	public SQLResponse prepareQuery(@RequestBody PrepareSqlRequest sqlRequest) {
		long startTimestamp = System.currentTimeMillis();

		SQLResponse response = doQuery(sqlRequest);
		response.setDuration(System.currentTimeMillis() - startTimestamp);

		queryService.logQuery(sqlRequest, response, new Date(startTimestamp),
				new Date(System.currentTimeMillis()));

		if (response.getIsException()) {
			String errorMsg = response.getExceptionMessage();
			throw new InternalErrorException(makeErrorMsgUserFriendly(errorMsg));
		}

		return response;
	}

	/**
	 * adjust error message order
	 * 
	 * @param errorMsg
	 * @return
	 */
	public String makeErrorMsgUserFriendly(String errorMsg) {
		try {
			errorMsg = errorMsg.replaceAll("\\s", " ");// replace all invisible
														// characters
			Pattern pattern = Pattern
					.compile("error while executing SQL \"(.*)\":(.*)");
			Matcher matcher = pattern.matcher(errorMsg);
			if (matcher.find()) {
				return matcher.group(2).trim() + "\n"
						+ "while executing SQL: \"" + matcher.group(1).trim()
						+ "\"";
			} else
				return errorMsg;
		} catch (Exception e) {
			return errorMsg;
		}
	}

	@RequestMapping(value = "/saved_queries", method = RequestMethod.POST)
	@ResponseBody
	@Timed(name = "saveQuery")
	public void saveQuery(@RequestBody SaveSqlRequest sqlRequest) {
		queryService.saveQuery(sqlRequest.getName(), sqlRequest.getProject(),
				sqlRequest.getSql(), sqlRequest.getDescription());
	}

	@RequestMapping(value = "/saved_queries/{id}", method = RequestMethod.DELETE)
	@ResponseBody
	@Timed(name = "removeQuery")
	public void removeQuery(@PathVariable String id) {
		queryService.removeQuery(id);
	}

	@RequestMapping(value = "/saved_queries", method = RequestMethod.GET)
	@ResponseBody
	@Timed(name = "getQueries")
	public List<GeneralResponse> getQueries() {
		String creator = SecurityContextHolder.getContext().getAuthentication()
				.getName();
		return queryService.getQueries(creator);
	}

	@RequestMapping(value = "/query/format/{format}", method = RequestMethod.GET)
	@ResponseBody
	@Timed(name = "downloadResult")
	public void downloadQueryResult(@PathVariable String format,
			SQLRequest sqlRequest, HttpServletResponse response) {
		SQLResponse result = doQuery(sqlRequest);
		response.setContentType("text/" + format + ";charset=utf-8");
		response.setHeader("Content-Disposition",
				"attachment; filename=\"result." + format + "\"");
		ICsvListWriter csvWriter = null;

		try {
			csvWriter = new CsvListWriter(response.getWriter(),
					CsvPreference.STANDARD_PREFERENCE);

			List<String> headerList = new ArrayList<String>();

			for (SelectedColumnMeta column : result.getColumnMetas()) {
				headerList.add(column.getName());
			}

			String[] headers = new String[headerList.size()];
			csvWriter.writeHeader(headerList.toArray(headers));

			for (List<String> row : result.getResults()) {
				csvWriter.write(row);
			}
		} catch (IOException e) {
			logger.error("", e);
		} finally {
			IOUtils.closeQuietly(csvWriter);
		}
	}

	@RequestMapping(value = "/tables_and_columns", method = RequestMethod.GET)
	@ResponseBody
	public List<TableMeta> getMetadata(MetaRequest metaRequest) {
		try {
			return queryService.getMetadata(metaRequest.getProject());
		} catch (SQLException e) {
			logger.error(e.getLocalizedMessage(), e);
			throw new InternalErrorException(e.getLocalizedMessage(), e);
		}
	}

	/**
	 * @param sqlRequest
	 * @return
	 */
	private SQLResponse doQuery(SQLRequest sqlRequest) {
		String sql = sqlRequest.getSql();
		String project = sqlRequest.getProject();
		logger.info("Using project: " + project);
		logger.info("The original query:  " + sql);

		// Check server mode.
		String serverMode = KylinConfig.getInstanceFromEnv().getServerMode();
		if (!(Constant.SERVER_MODE_QUERY.equals(serverMode.toLowerCase()) || Constant.SERVER_MODE_ALL
				.equals(serverMode.toLowerCase()))) {
			throw new InternalErrorException("Query is not allowed in "
					+ serverMode + " mode.");
		}

		try {
			if (sql.toLowerCase().contains("select")) {
				SQLResponse sqlResponse = null;

				Cache exceptionCache = cacheManager
						.getCache(EXCEPTION_QUERY_CACHE);
				Cache queryCache = cacheManager.getCache(SUCCESS_QUERY_CACHE);

				if (KylinConfig.getInstanceFromEnv().isQueryCacheEnabled()) {
					if (null != exceptionCache.get(sqlRequest)) {
						Element element = exceptionCache.get(sqlRequest);
						SQLResponse scanExceptionRes = (SQLResponse) element
								.getObjectValue();
						scanExceptionRes.setHitCache(true);

						return scanExceptionRes;
					}

					if (null != queryCache.get(sqlRequest)) {
						Element element = queryCache.get(sqlRequest);
						SQLResponse cachedRes = (SQLResponse) element
								.getObjectValue();
						cachedRes.setHitCache(true);

						return cachedRes;
					}
				}

				sqlResponse = queryService.query(sqlRequest);

				long durationThreshold = KylinConfig.getInstanceFromEnv()
						.getQueryDurationCacheThreshold();
				long scancountThreshold = KylinConfig.getInstanceFromEnv()
						.getQueryScanCountCacheThreshold();
				if (!sqlResponse.getIsException()
						&& (sqlResponse.getDuration() > durationThreshold || sqlResponse
								.getTotalScanCount() > scancountThreshold)) {
					queryCache.put(new Element(sqlRequest, sqlResponse));
				}

				if (!sqlResponse.getIsException()
						&& KylinConfig.getInstanceFromEnv()
								.isQuerySecureEnabled()) {
					CubeInstance cubeInstance = this.queryService
							.getCubeManager().getCube(sqlResponse.getCube());
					queryService.checkAuthorization(cubeInstance);
				}

				return sqlResponse;
			} else {
				logger.debug("Directly return expection as not supported");
				return new SQLResponse(null, null, 0, true,
						"Not Supported SQL.");
			}
		} catch (Exception e) {
			SQLResponse exceptionRes = new SQLResponse(null, null, 0, true,
					e.getMessage());
			Cache exceptionCache = cacheManager.getCache(EXCEPTION_QUERY_CACHE);
			exceptionCache.put(new Element(sqlRequest, exceptionRes));

			logger.error("Exception when execute sql", e);

			return new SQLResponse(null, null, 0, true, e.getMessage());
		}
	}

	public void setQueryService(QueryService queryService) {
		this.queryService = queryService;
	}

	public void setCacheManager(CacheManager cacheManager) {
		this.cacheManager = cacheManager;
	}

}