package co.mannit.commonservice.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import co.mannit.commonservice.ServiceCommonException;
import co.mannit.commonservice.common.MongokeyvaluePair;
import co.mannit.commonservice.common.util.ValidationUtil;
import co.mannit.commonservice.dao.ResourceDao;

@Service
public class ReadResource {

	private static final Logger logger = LogManager.getLogger(ReadResource.class);
			
	@Autowired
	private ResourceDao resourceDao;
	
	public List<String> readResource(String domain, String subDomain, String userId) throws Exception {
		logger.debug("<readResource>");
		
		if(Optional.of(ValidationUtil.validateDomainAndId(domain, subDomain, userId)).isEmpty()) {
			throw new ServiceCommonException("106");
		}
		
		List<MongokeyvaluePair<? extends Object>> lstKeyValuePairs = new ArrayList<>();
		lstKeyValuePairs.add(new MongokeyvaluePair<String>("domain", domain));
		lstKeyValuePairs.add(new MongokeyvaluePair<String>("subdomain", subDomain));
		lstKeyValuePairs.add(new MongokeyvaluePair<ObjectId>("userId", new ObjectId(userId)));
		
		
		List<Document> listDoc = resourceDao.findDoc(lstKeyValuePairs);
		
		List<String> lst = listDoc.stream().map(doc->doc.toJson()).collect(Collectors.toCollection(ArrayList::new));
		
		logger.debug("</readResource>");
//		return resourceDao.findDoc(lstKeyValuePairs);
//		return "Resource Created Successfully";
//		return listDoc;
		return lst;
	}
	
}
