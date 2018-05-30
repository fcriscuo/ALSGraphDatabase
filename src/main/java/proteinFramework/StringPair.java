package proteinFramework;

public class StringPair {
	protected final String _firstStr;
	protected final String _secondStr;

	public StringPair(String _first, String _second) {
		_firstStr = _first;
		_secondStr = _second;
	}

	public String getFirstString() {
		return _firstStr;
	}

	public String getSecondString() {
		return _secondStr;
	}
	
	

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((_firstStr == null) ? 0 : _firstStr.hashCode());
		result = prime * result
				+ ((_secondStr == null) ? 0 : _secondStr.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof StringPair)) {
			return false;
		}
		StringPair other = (StringPair) obj;
		if (_firstStr == null) {
			if (other._firstStr != null) {
				return false;
			}
		} else if (!_firstStr.equals(other._firstStr)) {
			return false;
		}
		if (_secondStr == null) {
			if (other._secondStr != null) {
				return false;
			}
		} else if (!_secondStr.equals(other._secondStr)) {
			return false;
		}
		return true;
	}

	public boolean isSimilar(Object obj) {
		if (!(obj instanceof StringPair))
			return false;
		StringPair p1 = (StringPair) obj;

		if ((p1._firstStr.equals(this._firstStr) && p1._secondStr
				.equals(this._secondStr))
				|| (p1._secondStr.equals(this._firstStr) && p1._firstStr
						.equals(this._secondStr)))

		{
			return (true);
		}

		return (false);
	}
}
