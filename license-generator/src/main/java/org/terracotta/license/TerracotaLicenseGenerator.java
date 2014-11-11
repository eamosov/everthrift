package org.terracotta.license;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.terracotta.license.License;
import org.terracotta.license.LicenseBuilder;
import org.terracotta.license.util.IOUtils;
import org.terracotta.license.util.Utils;


public class TerracotaLicenseGenerator {


	   public TerracotaLicenseGenerator() {

	   }

	   public License generateEvalLicense(InputStream privateKeyStream) throws Exception {
	      LicenseBuilder builder = new LicenseBuilder();
	      builder.setPrivateKeyInputStream(privateKeyStream);
	      builder.setProduct("BigMemory Go").setEdition("DX").setCapabilities("ehcache, TMC, ehcache offheap").setExpirationDate(this.getEvalExpirationDate()).setLicenseType("Trial");
	      builder.setLicenseNumber("0").setLicensee("Generic Evaluation License");
	      builder.addProperty("Max Client Count", "0");
	      builder.addProperty("ehcache.maxOffHeap", "32G");
	      return builder.createLicense();
	   }

	   private Date getEvalExpirationDate() {
	      return new Date(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(365 * 100L));
	   }

	   public static void main(String[] args) throws Exception {
//	      if(args.length == 0 || !(new File(args[0])).exists()) {
//	         System.out.println("Private key is needed as argument. Please pass the path to existing private key");
//	         System.exit(1);
//	      }

	      final InputStream privateKeyStream = TerracotaLicenseGenerator.class.getResourceAsStream("/license-private-key.x509");

	      try {
	    	  TerracotaLicenseGenerator generator = new TerracotaLicenseGenerator();
	         System.out.println(generator.generateEvalLicense(privateKeyStream).fullLicenseAsString());
	      } finally {
	         IOUtils.closeQuietly((InputStream)privateKeyStream);
	      }

	   }
}
