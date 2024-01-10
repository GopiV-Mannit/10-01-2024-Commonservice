package co.mannit.commonservice.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import co.mannit.commonservice.ServiceCommonException;
import co.mannit.commonservice.common.MongokeyvaluePair;
import co.mannit.commonservice.dao.ResourceDao;

@Service
public class SearchResource {

	private static final Logger logger = LogManager.getLogger(SearchResource.class);
	
	@Autowired
	private ResourceDao resourceDao;
	
	public List<String> searchResource(String domain, String subDomain, String userId, String searchString) throws Exception {
		logger.debug("<searchResource>");
		
		List<MongokeyvaluePair<? extends Object>> lstKeyValuePairs = new ArrayList<>();
		lstKeyValuePairs.add(new MongokeyvaluePair<String>("domain", domain));
		lstKeyValuePairs.add(new MongokeyvaluePair<String>("subdomain", subDomain));
		lstKeyValuePairs.add(new MongokeyvaluePair<ObjectId>("userId", new ObjectId(userId)));
		
		Map filters = parseFilter(searchString);
		
		List<Document> listDoc = resourceDao.search(lstKeyValuePairs, String.valueOf(filters.get("op")), (Map)filters.get("filter1"), (Map)filters.get("filter2"));;
		List<String> lst = listDoc.stream().map(doc->doc.toJson()).collect(Collectors.toCollection(ArrayList::new));
		
		logger.debug("<searchResource> size {}",lst.size());
		return lst;
	}
	
	private Map parseFilter(String search) throws ServiceCommonException {
		
		String[] tokens = search.split("\\s+");
		int lenght = tokens.length;
		Map filters = new HashMap<>();
		Map<String, String> filter = null;
		String[] nameop = null;
		if(lenght == 3) {
			
			nameop = tokens[0].split("_");
			filter = new HashMap<>();
			filter.put("name", nameop[0]);
			filter.put("dt", nameop[1]);
			filter.put("op", tokens[1]);
			filter.put("value", tokens[2]);
			filters.put("filter1", filter);
		}else if(lenght == 7) {
			nameop = tokens[0].split("_");
			filter = new HashMap<>();
			filter.put("name", nameop[0]);
			filter.put("dt", nameop[1]);
			filter.put("op", tokens[1]);
			filter.put("value", tokens[2]);
			filters.put("filter1", filter);
			
			filters.put("op", tokens[3]);
			
			nameop = tokens[4].split("_");
			filter = new HashMap<>();
			filter.put("name", nameop[0]);
			filter.put("dt", nameop[1]);
			filter.put("op", tokens[5]);
			filter.put("value", tokens[6]);
			filters.put("filter2", filter);
		}else {
			throw new ServiceCommonException(String.format("Invalid \"\\s\" filter", search));
		}
		
		logger.debug("parseFilter filter {}",  filters);
		return filters;
	}
	
}
