package org.hljcma.di.service;

/**
 * 
 * @author zzw
 * @desc redis service
 */
public interface DiRedisService {

	public void flushdb();

	public boolean set(String key, String value);
	
	public String get(String key);

	public boolean setlist(String key, String value);

	public String getlist(String key);

}