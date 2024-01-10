package co.mannit.commonservice.service;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import co.mannit.commonservice.ServiceCommonException;
import co.mannit.commonservice.common.MongokeyvaluePair;
import co.mannit.commonservice.common.ValueExtracterFromJSON;
import co.mannit.commonservice.common.util.DateUtil;
import co.mannit.commonservice.crypto.AESSymmetricEncryption;
import co.mannit.commonservice.dao.LogisignupassDao;
import co.mannit.commonservice.validator.PasswordValidator;

@Service
public class SignupService {

	private static final Logger logger = LogManager.getLogger(SignupService.class);
			
	@Autowired
	private ValueExtracterFromJSON valueExtracterFromJSON;
	
	@Autowired
	private LogisignupassDao logisignupassDao;
	
	@Autowired
	private AESSymmetricEncryption aesSymmetricEncryption;
	
	@Autowired
	private PasswordValidator passwordValidator;
	
	Pattern ptrn = Pattern.compile("^\\d{10}$");
	
	public String signupUser(String registrationDetails) throws Exception{
		logger.debug("<signupUser> registrationDetails:{}",registrationDetails);
		
		String userName = valueExtracterFromJSON.getValue(registrationDetails, "username", String.class);
		Long mobileno = valueExtracterFromJSON.getValue(registrationDetails, "mobileno", Long.class);
		String password = String.valueOf(valueExtracterFromJSON.getValue(registrationDetails, "password", String.class));
		
		String domain = String.valueOf(valueExtracterFromJSON.getValue(registrationDetails, "domain", String.class));
		String subDomain = String.valueOf(valueExtracterFromJSON.getValue(registrationDetails, "subdomain", String.class));
//		String email = valueExtracterFromJSON.getValue(registrationDetails, "email", String.class);
		
//		logisignupassDao.printAllDoc();
		
		//how to validate the domain name
		if(!StringUtils.hasText(domain) || !StringUtils.hasText(subDomain)) {
			throw new ServiceCommonException("100");
		}
		
        if(!StringUtils.hasText(userName) && (mobileno == null || !StringUtils.hasText(String.valueOf(mobileno)))) {
        	throw new ServiceCommonException("102");
        }
		
        if (!StringUtils.hasText(password)) {
            throw new ServiceCommonException("102");
        }
        
//		if(!StringUtils.hasText(userName) || !StringUtils.hasText(password)) {
//			throw new ServiceCommonException("102");
//		}
		
		if(mobileno != null && StringUtils.hasText(String.valueOf(mobileno)) && !ptrn.matcher(String.valueOf(mobileno)).matches()) {
			throw new ServiceCommonException("103", new String[]{"mobile number"});
		}
		
		if(!passwordValidator.isValid(password)) {throw new ServiceCommonException("105");}
		
		if(userName != null && StringUtils.hasText(String.valueOf(userName)) && logisignupassDao.isUserAlreadyExist("username", userName)) {
			throw new ServiceCommonException("104", new String[] {"User name"});
		}

		if(mobileno != null && StringUtils.hasText(String.valueOf(mobileno)) && logisignupassDao.isUserAlreadyExist("mobileno", mobileno)) {
			throw new ServiceCommonException("104", new String[] {"User mobile number"});
		}
		
		logger.debug("aesSymmetricEncryption.getSecretKey():{}",aesSymmetricEncryption.getSecretKey());
		List<MongokeyvaluePair<? extends Object>> keyValuePairs = new ArrayList<>();
		keyValuePairs.add(new MongokeyvaluePair<String>("password", aesSymmetricEncryption.encryptAsString(password, aesSymmetricEncryption.getSecretKey()) ));
		keyValuePairs.add(new MongokeyvaluePair<String>("creationdate", DateUtil.getCurrentDateTime()));
		keyValuePairs.add(new MongokeyvaluePair<String>("updated_at", DateUtil.getCurrentDateTime()));
		
		logisignupassDao.insertDocument(registrationDetails, keyValuePairs);
		
		logger.debug("</signupUser>");
		return "User Successfully Registered";
	}
}
