package co.mannit.commonservice.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.DocumentCallbackHandler;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import com.mongodb.MongoException;

import co.mannit.commonservice.common.MongokeyvaluePair;

@Repository
public class CommonDao {

	private static final Logger logger = LogManager.getLogger(CommonDao.class);
			
	@Autowired
	private MongoTemplate mongoTemplate;
	
	public boolean isUserAlreadyExist(String colName, String key, Object value) {
		logger.debug("<isUserAlreadyExist> colName:{} key:{} value:{}",colName,key,value);
		
		boolean isExist = false;
		
		Query query = new Query();
		query.addCriteria(Criteria.where(key).is(value));
		isExist = mongoTemplate.exists(query, colName);
		
		logger.debug("</isUserAlreadyExist> isExist:{}",isExist);
		return isExist;
	}
	
	public boolean isUserAlreadyExist(String colName, MongokeyvaluePair<? extends Object> pair) {
		logger.debug("<isUserAlreadyExist> colName:{} pair:{}",colName, pair);
		
		boolean isExist = false;
		
		Query query = new Query();
		query.addCriteria(Criteria.where(pair.getKey()).is(pair.getValue()));
		isExist = mongoTemplate.exists(query, colName);
		
		logger.debug("</isUserAlreadyExist> isExist:{}",isExist);
		return isExist;
	}
	
	public String findOneDoc(String colName, List<MongokeyvaluePair<? extends Object>>  keyValuePairs) throws Exception {
		logger.debug("<findDoc> colName:{} keyValuePairs:{}",colName,keyValuePairs);
		
		Query query = new Query();
		if(keyValuePairs != null && keyValuePairs.size() > 0) {
			
			keyValuePairs.forEach(pair -> {
				query.addCriteria(Criteria.where(pair.getKey()).is(pair.getValue()));
			});
			
			/*Stream.of(keyValuePairs).forEach(pair -> {
			  query.addCriteria(Criteria.where(pair.getKey()).is(pair.getValue())); 
			  });*/

		}
		
		List<Document> lsftDoc = new ArrayList<>();
		mongoTemplate.executeQuery(query, colName, new DocumentCallbackHandler() {
			@Override
			public void processDocument(Document document) throws MongoException, DataAccessException {
				lsftDoc.add(document);
			}
		});
		
		if(lsftDoc.size() > 1) {
			throw new Exception("More than one document found for keyValuePairs:{}".formatted(keyValuePairs)); 
		}
		
		String doc = null;
		if(lsftDoc.size() == 1) {
			doc = lsftDoc.get(0).toJson();
		}
		
		logger.debug("</isUserAlreadyExist> Doc:{}",doc);
		return doc;
	}
	
	public String findOneDoc(String colName, MongokeyvaluePair<? extends Object>  keyValuePairs) throws Exception {
		List<MongokeyvaluePair<? extends Object>> lstKeyValuePairs = new ArrayList<>();
		lstKeyValuePairs.add(keyValuePairs);
		return findOneDoc(colName, lstKeyValuePairs);
	}
	
	public void insertDocument(String colName, String json) {
		logger.debug("<insertDocument> colName:{} json:{}",colName,json);
		mongoTemplate.insert(json, colName);
		logger.debug("</insertDocument>");
	}
	
	public void insertDocument(String colName, String json, List<MongokeyvaluePair<? extends Object>> keyValuPairs) {
		logger.debug("<insertDocument> colName:{} json:{} keyValuPairs:{}",colName,json, keyValuPairs);
		
		Document document = Document.parse(json);
		
		if(keyValuPairs != null && keyValuPairs.size()>0) {
			keyValuPairs.stream().forEach(pair -> {
				document.append(pair.getKey(), pair.getValue());
			});
		}
		
		/*
		 * Optional.of(keyValuPairs).flatMap(lstKeyValue ->
		 * lstKeyValue.stream().forEach(pair->{ document.append(pair.getKey(),
		 * pair.getValue()); }));
		 */
		
		/*
		 * stream().forEach(pair ->{ document.append(pair.g, keyValuPairs) });
		 */

		
		mongoTemplate.insert(document, colName);
		logger.debug("</insertDocument>");
	}
	
	public void saveDocument(String colName, String json, List<MongokeyvaluePair<? extends Object>> keyValuPairs) {
		logger.debug("<saveDocument> colName:{} json:{} keyValuPairs:{}",colName,json, keyValuPairs);
		
		Document document = Document.parse(json);
		
		if(keyValuPairs != null && keyValuPairs.size()>0) {
			keyValuPairs.stream().forEach(pair -> {
				document.append(pair.getKey(), pair.getValue());
			});
		}
		
		mongoTemplate.save(document, colName);
		logger.debug("</saveDocument>");
	}
	
	public void printAllDoc(String colName) {
		logger.debug("<printAllDoc> colName:{}",colName);
		
		mongoTemplate.executeQuery(new Query(), "user", new DocumentCallbackHandler() {

			@Override
			public void processDocument(org.bson.Document document) throws MongoException, DataAccessException {
				// TODO Auto-generated method stub
				logger.debug(document);
			}
			
		});
		
		logger.debug("</printAllDoc>");
	}
	
	public List<Document> findDoc(String colName, List<MongokeyvaluePair<? extends Object>>  keyValuePairs) throws Exception {
		logger.debug("<findDoc> colName:{} keyValuePairs:{}",colName,keyValuePairs);
		
		Query query = new Query();
		if(keyValuePairs != null && keyValuePairs.size() > 0) {
			
			keyValuePairs.forEach(pair -> {
				query.addCriteria(Criteria.where(pair.getKey()).is(pair.getValue()));
			});
		}
		
		List<Document> lsftDoc = new ArrayList<>();
		mongoTemplate.executeQuery(query, colName, new DocumentCallbackHandler() {
			@Override
			public void processDocument(Document document) throws MongoException, DataAccessException {
				lsftDoc.add(document);
			}
		});

		
		logger.debug("</isUserAlreadyExist> Doc:{}",lsftDoc.size());
		return lsftDoc;
	}
	
	public List<Document> finDoc(String colName, MongokeyvaluePair<? extends Object>  keyValuePairs) throws Exception {
		List<MongokeyvaluePair<? extends Object>> lstKeyValuePairs = new ArrayList<>();
		lstKeyValuePairs.add(keyValuePairs);
		return findDoc(colName, lstKeyValuePairs);
	}
	
	public void deleteDoc(String colName, List<MongokeyvaluePair<? extends Object>>  keyValuePairs) throws Exception {
		logger.debug("<deleteDoc> colName:{} keyValuePairs:{}",colName,keyValuePairs);
		
		Query query = new Query();
		if(keyValuePairs != null && keyValuePairs.size() > 0) {
			
			keyValuePairs.forEach(pair -> {
				query.addCriteria(Criteria.where(pair.getKey()).is(pair.getValue()));
			});
		}
		mongoTemplate.remove(query, colName);
		logger.debug("</deleteDoc>");
	}
	
	public void replaceDoc(String colName, String json, List<MongokeyvaluePair<? extends Object>>  fields, List<MongokeyvaluePair<? extends Object>>  criteria) throws Exception {
		logger.debug("<replaceDoc> colName:{} json:{} keyValuePairs:{}",colName,json, criteria);
		
		Document document = Document.parse(json);
		
		if(fields != null && fields.size()>0) {
			fields.stream().forEach(pair -> {
				document.append(pair.getKey(), pair.getValue());
			});
		}
		
		Query query = new Query();
		if(criteria != null && criteria.size() > 0) {
			
			criteria.forEach(pair -> {
				query.addCriteria(Criteria.where(pair.getKey()).is(pair.getValue()));
			});
		}
		mongoTemplate.findAndReplace(query, document, colName);
		logger.debug("</replaceDoc>");
	}
	
	
	public List<Document> search(String colName, List<MongokeyvaluePair<? extends Object>>  keyValuePairs, String op, Map<String, String> filter1, Map<String, String> filter2) throws Exception {
		logger.debug("<findDoc> colName:{} keyValuePairs:{}",colName,keyValuePairs);
		
		Query query = new Query();
		if(keyValuePairs != null && keyValuePairs.size() > 0) {
			
			keyValuePairs.forEach(pair -> {
				query.addCriteria(Criteria.where(pair.getKey()).is(pair.getValue()));
			});
			
			if(op == null || "null".equals(op)) {
				query.addCriteria(Criteria.where(filter1.get("name")).is(dataTypeConverter.get(filter1.get("dt")).apply(filter1.get("value"))));
			}else if("or".equals(op)) {
				query.addCriteria(
						new Criteria().orOperator(Criteria.where(filter1.get("name")).is(dataTypeConverter.get(filter1.get("dt")).apply(filter1.get("value"))),
								Criteria.where(filter2.get("name")).is(dataTypeConverter.get(filter2.get("dt")).apply(filter2.get("value")))));
			}else if("and".equals(op)) {
				query.addCriteria(
						new Criteria().andOperator(Criteria.where(filter1.get("name")).is(dataTypeConverter.get(filter1.get("dt")).apply(filter1.get("value"))),
								Criteria.where(filter2.get("name")).is(dataTypeConverter.get(filter2.get("dt")).apply(filter2.get("value")))));
			}
			
		}
		
		List<Document> lsftDoc = new ArrayList<>();
		mongoTemplate.executeQuery(query, colName, new DocumentCallbackHandler() {
			@Override
			public void processDocument(Document document) throws MongoException, DataAccessException {
				lsftDoc.add(document);
			}
		});

		logger.debug("</isUserAlreadyExist> Doc:{}",lsftDoc.size());
		return lsftDoc;
	}
	
	
	private Map<String, Function<String, ? extends Object>> dataTypeConverter = null;
	{
		dataTypeConverter = new HashMap<>();
		dataTypeConverter.put("L", (t)-> Long.parseLong(t));
		dataTypeConverter.put("I", (t)-> Integer.parseInt(t));
		dataTypeConverter.put("S", (t)-> t);
	}
	

}
