package co.mannit.commonservice.controller;

import java.util.Base64;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import co.mannit.commonservice.common.Response;
import co.mannit.commonservice.service.CreateResource;
import co.mannit.commonservice.service.DeleteResource;
import co.mannit.commonservice.service.ReadResource;
import co.mannit.commonservice.service.SearchResource;
import co.mannit.commonservice.service.UpdateResource;

@RestController
public class ResourceController {

	private static final Logger logger = LogManager.getLogger(ResourceController.class);
	
	@Autowired
	private CreateResource createService;
	
	@Autowired
	private ReadResource readResource;
	
	@Autowired
	private UpdateResource updateResource;
	
	@Autowired
	private DeleteResource deleteResource;
	
	@Autowired
	private SearchResource searchResource;
	
	@PostMapping("eCreate")
	public Response<?> createResource(@RequestBody String json, @RequestParam("domain")String domain, @RequestParam("subdomain")String subdomain, @RequestParam("userId")String userId) throws Exception {
		logger.debug("<createResource> domain {} subdomain {} userId {} json{}",domain,subdomain, userId,json);
		
		String msg = createService.createResource(domain, subdomain, userId, json);
		
		return Response.buildSuccessMsg(200, msg, null);
	}
	
	@PutMapping("eUpdate")
	public Response<?> updateResource(@RequestBody String json, @RequestParam("domain")String domain, @RequestParam("subdomain")String subdomain, @RequestParam("userId")String userId, @RequestParam("resourceId")String resourceId) throws Exception {
		logger.debug("<updateResource> domain {} subdomain {} userId {} resourceId{} json{}",domain,subdomain, userId, resourceId, json);
		updateResource.updateResource(domain, subdomain, userId, resourceId, json);
		return Response.buildSuccessMsg(200, "Resource Updated Successfully", null);
	}
	
	@GetMapping("eRead")
	public Response<?> readResource(@RequestParam("domain")String domain, @RequestParam("subdomain")String subdomain, @RequestParam("userId")String userId) throws Exception {
		logger.debug("<readResource> domain {} subdomain {} userId {} ",domain,subdomain, userId);
		return Response.buildSuccessMsg(200, "Read Successfully", readResource.readResource(domain, subdomain, userId));
	}
	
	@DeleteMapping("eDelete")
	public Response<?> deleteResource(@RequestParam("domain")String domain, @RequestParam("subdomain")String subdomain, @RequestParam("resourceId")String resourceId) throws Exception {
		logger.debug("<deleteResource> domain {} subdomain {} resourceId {}",domain,subdomain, resourceId);
		
		deleteResource.deleteResource(domain, subdomain, resourceId);
		
		return Response.buildSuccessMsg(200, "Deleted Successfully", null);
	}
	
	@GetMapping("eSearch")
	public Response<?> searchResource(@RequestParam("domain")String domain, @RequestParam("subdomain")String subdomain, @RequestParam("userId")String userId, @RequestParam("f")String filter) throws Exception {
		logger.debug("<searchResource> domain {} subdomain {} userId {}",domain,subdomain, userId);
		
		return Response.buildSuccessMsg(200, "Successfully Searched", searchResource.searchResource(domain, subdomain, userId, filter));
	}
	
	//authorization : Basic YWJjZGVmMzQ6YWJjZGZncmVHMUA=
	private String[] parseAuthentication(String auth) {
		
		String[] authArray = null;
		
		String authToken = auth.substring(auth.indexOf(" ")+1, auth.length());
		authToken = new String(Base64.getDecoder().decode(authToken));
		if(StringUtils.hasText(auth)) {
			
			String userName = authToken.substring(0, authToken.indexOf(":"));
			String password = authToken.substring(authToken.indexOf(":")+1, authToken.length());
			authArray = new String[]{userName, password};
		}
		
		return authArray;
	}
	
	public static void main(String[] args) {
//		System.out.println(Arrays.toString(parseAuthentication("Basic YWJjZGVmMzQ6YWJjZGZncmVHMUA=")));
		
		/*System.out.println(request.getAuthType());
		Enumeration<String> headers = request.getHeaderNames();
		while(headers.hasMoreElements()) {
			String name = headers.nextElement();
//			System.out.println(name);
			System.out.println(name+" : "+request.getHeader(name));
		}*/
	}
}
