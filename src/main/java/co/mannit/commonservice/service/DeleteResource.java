package co.mannit.commonservice.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import co.mannit.commonservice.ServiceCommonException;
import co.mannit.commonservice.common.MongokeyvaluePair;
import co.mannit.commonservice.common.util.ValidationUtil;
import co.mannit.commonservice.dao.ResourceDao;

@Service
public class DeleteResource {

	private static final Logger logger = LogManager.getLogger(DeleteResource.class);
			
	@Autowired
	private ResourceDao resourceDao;
	
	public void deleteResource(String domain, String subDomain, String resourceId) throws Exception {
		logger.debug("<deleteResource>");
		
		if(Optional.of(ValidationUtil.validateDomainAndId(domain, subDomain, resourceId)).isEmpty()) {
			throw new ServiceCommonException("106");
		}
		
		List<MongokeyvaluePair<? extends Object>> lstKeyValuePairs = new ArrayList<>();
		lstKeyValuePairs.add(new MongokeyvaluePair<String>("domain", domain));
		lstKeyValuePairs.add(new MongokeyvaluePair<String>("subdomain", subDomain));
		lstKeyValuePairs.add(new MongokeyvaluePair<ObjectId>("_id", new ObjectId(resourceId)));
		
		
		resourceDao.deleteDoc(lstKeyValuePairs);
		logger.debug("</deleteResource>");
	}
	
}
