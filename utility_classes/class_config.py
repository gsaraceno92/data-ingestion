try:
    import configparser as ConfigParser
except:
	#exception to handle the use of Python 2.*
    import ConfigParser

class objConfig(object):
	def __init__(self, cfgfile):
		self.sections = {}
		cfg = ConfigParser.RawConfigParser()
		if cfg is not None:
			cfg.read(cfgfile)
			sections = cfg.sections()
			for section in sections:
				cfgItems = {}
				sectionItems = cfg.items(section)
				for item in sectionItems:
					cfgItems[item[0]] = item[1]
				self.sections[section] = cfgItems
	