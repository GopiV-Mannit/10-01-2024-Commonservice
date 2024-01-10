package co.mannit.commonservice.common.util;

import org.springframework.util.StringUtils;

public class ValidationUtil {

	static public boolean validateDomainAndId(String domain, String subDomain, String id) {
		if(!StringUtils.hasLength(domain) || !StringUtils.hasLength(subDomain) || !StringUtils.hasLength(id)) {
			return false;
		}
		
		return true;
	}
}
